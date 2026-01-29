import base64
import gzip
import io
import json
import traceback
from datetime import datetime, timezone

import fastavro
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import sqlalchemy as sa
from astropy.io import fits
from astropy.visualization import (AsymmetricPercentileInterval,
                                   ImageNormalize, LinearStretch, LogStretch)
from confluent_kafka import Consumer, KafkaError
from scipy.ndimage import rotate
from sqlalchemy.orm.session import Session

from baselayer.app.env import load_env
from baselayer.app.models import init_db
from baselayer.log import make_log
from skyportal.handlers.api.photometry import add_external_photometry
from skyportal.handlers.api.thumbnail import post_thumbnail
from skyportal.models import (Annotation, Candidate, DBSession, Filter,
                              Instrument, Obj, Stream, User)

matplotlib.pyplot.switch_backend("Agg")

env, cfg = load_env()
log = make_log("boom")

init_db(**cfg["database"])

params = cfg.get("services.external.boom.params", {})
thumbnail_types = [
    ("cutoutScience", "new"),
    ("cutoutTemplate", "ref"),
    ("cutoutDifference", "sub"),
]

def make_thumbnail(
    obj_id, cutout_data, cutout_type: str, thumbnail_type: str, survey: str
):
    rotpa = None
    if survey == "LSST": # LSST uses no compression
        with fits.open(
            io.BytesIO(cutout_data), ignore_missing_simple=True
        ) as hdu:
            rotpa = hdu[0].header.get("ROTPA", None)
            data = hdu[0].data
    else:
        with (
            gzip.open(io.BytesIO(cutout_data), "rb") as f,
            fits.open(
                io.BytesIO(f.read()), ignore_missing_simple=True
            ) as hdu,
        ):
            rotpa = hdu[0].header.get("ROTPA", None)
            data = hdu[0].data

    buff = io.BytesIO()
    plt.close("all")
    fig = plt.figure()
    fig.set_size_inches(4, 4, forward=False)
    ax = plt.Axes(fig, [0.0, 0.0, 1.0, 1.0])
    ax.set_axis_off()
    fig.add_axes(ax)

    # Clean the data
    img = np.array(data)
    xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
    if img[xl].any():
        img[xl] = np.nan
    if np.isnan(img).any():
        median = float(np.nanmean(img.flatten()))
        img = np.nan_to_num(img, nan=median)

    # Normalize
    stretch = LinearStretch() if cutout_type == "cutoutDifference" else LogStretch()
    norm = ImageNormalize(img, stretch=stretch)
    img_norm = norm(img)

    normalizer = AsymmetricPercentileInterval(
        lower_percentile=1, upper_percentile=100
    )
    vmin, vmax = normalizer.get_limits(img_norm)

    # Survey-specific transformations to get North up and West on the right
    if survey == "ZTF":
        # flip the image in the vertical direction
        img_norm = np.flipud(img_norm)
    elif survey == "LSST":
        try:
            # Rotate clockwise by ROTPA degrees, reshape to avoid cropping, fill blanks with 0
            img_norm = rotate(
                img_norm,
                -rotpa,
                reshape=True,
                order=1,
                mode="constant",
                cval=0.0,
            )
        except Exception as e:
            # If scipy is not available or rotation fails, skip rotation
            log(f"Failed to rotate LSST image for obj_id {obj_id}: {e}")

    ax.imshow(img_norm, cmap="bone", origin="lower", vmin=vmin, vmax=vmax)
    plt.savefig(buff, dpi=42)

    buff.seek(0)
    plt.close("all")

    thumbnail_dict = {
        "obj_id": obj_id,
        "data": base64.b64encode(buff.read()).decode("utf-8"),
        "ttype": thumbnail_type,
    }

    return thumbnail_dict

def add_thumbnails(alert, survey, session):
    for cutout_type, thumbnail_type in thumbnail_types:
        if cutout_type not in alert:
            log(f"Cutout key {cutout_type} not found in alert")
            continue
        try:
            thumbnail = make_thumbnail(
                alert["objectId"], alert[cutout_type], cutout_type, thumbnail_type, survey
            )
        except Exception as e:
            traceback.print_exc()
            log(f"Failed to create thumbnail for cutout type {cutout_type}: {e}")
            continue
        post_thumbnail(thumbnail, user_id=1, session=session)
    

def read_avro(msg):
    """
    Reads an Avro file and returns the first record

    Args:
        file_path (str): The path to the Avro file.

    Returns:
        dict: The record read from the Avro file, or None if an error occurs.
    """

    bytes_io = io.BytesIO(msg.value())  # Get the message value as bytes
    bytes_io.seek(0)
    for record in fastavro.reader(bytes_io):
        return record  # Return the first record found
    return None  # Return None if no records are found or if an error occurs

def make_programid2stream_mapper(session: Session):
    # here we:
    # - get all the streams
    # - each stream has an altdata field that looks like: "`{'collection': 'ZTF_alerts', selector: [1, 2]}`"
    # - using the altdata's content we create a mapper where given a survey name and a programid we get the streams
    # - basically each stream with a given survey name and programid in its selector is associated with a programid
    streams = session.scalars(sa.select(Stream)).all()
    mapper = {}
    for stream in streams:
        altdata = stream.altdata
        survey = altdata['collection'].split('_')[0]
        programid = max(altdata['selector'])
        key = (survey, programid)
        if key not in mapper:
            mapper[key] = set()
        mapper[(survey, programid)].add(stream.id)

    # convert from set to list
    for key in mapper:
        mapper[key] = list(mapper[key])
    return mapper

def make_survey2instrumentid(session: Session):
    ztf_instrument_id = session.scalar(sa.select(Instrument.id).where(Instrument.name == 'ZTF'))
    if ztf_instrument_id is None:
        raise ValueError("Instrument ZTF not found in the database")
    lsst_instrument_id = session.scalar(sa.select(Instrument.id).where(Instrument.name == 'LSST'))
    if lsst_instrument_id is None:
        raise ValueError("Instrument LSST not found in the database")
    survey2instrumentid = {
        "ZTF": ztf_instrument_id,
        "LSST": lsst_instrument_id
    }
    return survey2instrumentid

def main():
    # first let's grab the instrument id for ZTF from the database
    with DBSession() as session:
        user = session.scalar(sa.select(User).where(User.id == 1))
        if user is None:
            log("User with id 1 not found in the database")
            return
        try:
            survey2instrumentid = make_survey2instrumentid(session)
        except ValueError as e:
            log(str(e))
            return
        programid2streamid = make_programid2stream_mapper(session)

        # get all filters
        all_filters = session.scalars(sa.select(Filter)).all()
        # only keep those where the Filter `altdata` has a boom key
        boom_filters: list[Filter] = [f for f in all_filters if f.altdata is not None and 'boom' in f.altdata]
        boom_filters = {f.altdata['boom']['filter_id']: {**f.to_dict(), 'group': f.group.to_dict()} for f in boom_filters}
        if not boom_filters:
            log("No boom filters found")
            return

    # TODO: validate params
    kafka_config = {
        "bootstrap.servers": f"{params.get('kafka_host', 'localhost')}:{params.get('kafka_port', 9092)}",  # Kafka server and port
        "group.id": params.get('kafka_group_id', 'my_group'),  # Consumer group ID
        "auto.offset.reset": "earliest",  # Start reading from the earliest message (DEBUG)
        "enable.auto.commit": False,  # Disable auto-commit of offsets
        "session.timeout.ms": 6000,  # Session timeout for the consumer
        "max.poll.interval.ms": 300000,  # Maximum time between polls
        "security.protocol": "PLAINTEXT",  # Use PLAINTEXT if no authentication
    }

    kafka_username, kafka_password = params.get("kafka_username"), params.get("kafka_password")
    kafka_sasl_mechanism = params.get("kafka_sasl_mechanism", "PLAIN")
    if kafka_username and kafka_password:
        # validate that sasl mechanism is one of the supported ones
        if kafka_sasl_mechanism not in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]:
            log(f"Unsupported SASL mechanism: {kafka_sasl_mechanism}")
            return
        kafka_config.update(
            {
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": kafka_sasl_mechanism,
                "sasl.username": kafka_username,
                "sasl.password": kafka_password,
            }
        )
    
    print(f"Connecting to Kafka at {kafka_config['bootstrap.servers']} (group ID: {kafka_config['group.id']})")
    # Create a Kafka consumer instance with the configuration
    consumer = Consumer(kafka_config)
    # Subscribe to the topic ZTF_alerts_results
    topic_names = params.get("topics", ["ZTF_alerts_results", "LSST_alerts_results"])  # Replace with your topic names
    consumer.subscribe(topic_names)  # Subscribe to the topics
    log(f"Subscribed to topics: {topic_names}")
    # Poll for messages from the topic
    while True:
        msg = consumer.poll(timeout=10.0)
        if msg is None:
            log("No message received within the timeout period.")
            continue
        if msg.error():
            # Handle any errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log("End of partition reached.")
                continue
            else:
                log(f"Error: {msg.error()}")
                continue
        
        # Successfully received a message
        record = read_avro(msg)

        with DBSession() as session:
            obj_id = record['objectId']
            survey = record['survey']
            obj = session.scalar(sa.select(Obj).where(Obj.id == obj_id))
            if obj is None:
                ra, dec = record['ra'], record['dec']
                obj = Obj(
                    id=obj_id,
                    ra=ra,
                    dec=dec,
                    ra_dis=ra,
                    dec_dis=dec,
                )
                session.add(obj)
                log(f"Created object with id {obj_id}")

                # add thumbnails
                add_thumbnails(record, survey.upper(), session)
            else:
                log(f"Object with id {obj_id} already exists")

            # we checked which candidates are already created
            candid = record['candid']
            passed_filter_ids = session.scalars(
                sa.select(
                    Candidate.filter_id,
                ).where(Candidate.passing_alert_id == candid)
            ).all()
            passed_filter_ids = set(passed_filter_ids)

            for filter_data in record['filters']:
                filt = boom_filters.get(filter_data['filter_id'])
                if filt is None:
                    log(f"Filter with id {filter_data['filter_id']} does not exist")
                    continue
                if filt['id'] not in passed_filter_ids:
                    # create the candidate if it's not already created
                    try:
                        candidate = Candidate(
                            obj=obj,
                            filter_id=filt['id'],
                            # convert passed_at from a timestamp in milliseconds to a datetime object
                            passed_at=datetime.fromtimestamp(filter_data['passed_at'] / 1000, timezone.utc),
                            passing_alert_id=candid,
                            uploader_id=1
                        )
                        session.add(candidate)
                        session.commit()
                    except Exception as e:
                        log(f"Error creating candidate with candid {candid}: {e}")
                        session.rollback()
                        continue
                    log(f"Created candidate with candid {candid}")
                else:
                    log(f"Skipping candidate with candid {candid}")

                # for each filter we get the "annotations" which is a JSON string, we parse it
                annotation_data = json.loads(filter_data['annotations'])

                group_name = filt['group'].get('nickname')
                if group_name is None: # if nickname is not present, use the name
                    group_name = filt['group']['name']
                origin = f"{group_name}:{filt['name']}"

                existing_annotation = session.scalar(
                    sa.select(Annotation).filter(
                        Annotation.obj_id == obj_id,
                        Annotation.origin == origin
                    )
                )
                if existing_annotation is None:
                    annotation = Annotation(
                        obj=obj,
                        data=annotation_data,
                        origin=origin,
                        author_id=1
                    )
                    session.add(annotation)
                    log(f"Created annotation with origin {origin}")
                else:
                    # we update the data of the annotation
                    existing_annotation.data = annotation_data
                    log(f"Updated annotation with origin {origin}")

            session.commit()

            # we group the photometry by survey and programid
            photometry_data = {}
            for phot in record['photometry']:
                # TEMPORARY: ignore forced photometry
                if phot['origin'] == 'ForcedPhot':
                    continue
                if phot['flux'] == -99999.0 or phot['flux_err'] == -99999.0:
                    continue
                key = (phot['survey'], phot['programid'])
                if key not in photometry_data:
                    stream_ids = programid2streamid.get(key)
                    if stream_ids is None:
                        log(f"No stream found for survey {phot['survey']} and programid {phot['programid']}, skipping photometry")
                        continue
                    instrument_id = survey2instrumentid.get(phot['survey'])
                    if instrument_id is None:
                        log(f"No instrument found for survey {phot['survey']}, skipping photometry")
                        continue
                    photometry_data[key] = {
                        'obj_id': obj_id,
                        'group_ids': [1],
                        'stream_ids': stream_ids,
                        'instrument_id': instrument_id,
                        'mjd': [],
                        'flux': [],
                        'fluxerr': [],
                        'filter': [],
                        'zp': [],
                        'magsys': [],
                        'ra': [],
                        'dec': [],
                    }
                photometry_data[key]['mjd'].append(phot['jd'] - 2400000.5)
                flux = phot['flux']
                if flux is not None and not np.isnan(flux):
                    flux = flux * 1e-9
                photometry_data[key]['flux'].append(flux)
                photometry_data[key]['fluxerr'].append(phot['flux_err'] * 1e-9)
                photometry_data[key]['filter'].append(phot['band'])
                photometry_data[key]['zp'].append(phot['zero_point'])
                photometry_data[key]['magsys'].append('ab')
                photometry_data[key]['ra'].append(phot['ra'])
                photometry_data[key]['dec'].append(phot['dec'])

            for key, data in photometry_data.items():
                add_external_photometry(data, user, session)

            session.commit()


if __name__ == "__main__":
    main()