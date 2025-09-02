import fastavro
import sqlalchemy as sa
from sqlalchemy.orm.session import Session
import json
from datetime import datetime, timezone
import gzip
import io
import base64
import snappy
from confluent_kafka import Consumer, KafkaError

import matplotlib
import matplotlib.pyplot as plt
from astropy.io import fits
from astropy.visualization import (
    AsymmetricPercentileInterval,
    ImageNormalize,
    LinearStretch,
    LogStretch,
)
import numpy as np

from baselayer.app.env import load_env
from baselayer.app.models import init_db
from baselayer.log import make_log
from skyportal.models import (
    DBSession,
    Obj,
    Candidate,
    Filter,
    Annotation,
    Instrument,
    User,
    Stream,
)
from skyportal.handlers.api.photometry import add_external_photometry
from skyportal.handlers.api.thumbnail import post_thumbnail

matplotlib.pyplot.switch_backend("Agg")

env, cfg = load_env()
log = make_log("boom")

init_db(**cfg["database"])

params = cfg.get("plugins.boom.params", {})

thumbnail_types = [
    ("cutoutScience", "new"),
    ("cutoutTemplate", "ref"),
    ("cutoutDifference", "sub"),
]

def make_thumbnail(
    obj_id, cutout_data, cutout_type: str, thumbnail_type: str, survey: str
):
    if survey == "LSST": # LSST uses snappy instead of gzip
        with snappy.decompress(cutout_data) as f:
            with fits.open(io.BytesIO(f), ignore_missing_simple=True) as hdu:
                image_data = hdu[0].data
    else:
        with gzip.open(io.BytesIO(cutout_data), "rb") as f:
            with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                image_data = hdu[0].data

    # Survey-specific transformations to get North up and West on the right
    if survey == "ZTF":
        image_data = np.flipud(image_data)

    buff = io.BytesIO()
    plt.close("all")
    fig = plt.figure()
    fig.set_size_inches(4, 4, forward=False)
    ax = plt.Axes(fig, [0.0, 0.0, 1.0, 1.0])
    ax.set_axis_off()
    fig.add_axes(ax)

    # replace nans with median:
    img = np.array(image_data)
    # replace dubiously large values
    xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
    if img[xl].any():
        img[xl] = np.nan
    if np.isnan(img).any():
        median = float(np.nanmean(img.flatten()))
        img = np.nan_to_num(img, nan=median)

    norm = ImageNormalize(
        img,
        stretch=LinearStretch()
        if cutout_type == "cutoutDifference"
        else LogStretch(),
    )
    img_norm = norm(img)
    normalizer = AsymmetricPercentileInterval(
        lower_percentile=1, upper_percentile=100
    )
    vmin, vmax = normalizer.get_limits(img_norm)
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

def main():
    # first let's grab the instrument id for ZTF from the database
    with DBSession() as session:
        user = session.scalar(sa.select(User).where(User.id == 1))
        if user is None:
            log("User with id 1 not found in the database")
            return
        instrument_id = session.scalar(sa.select(Instrument.id).where(Instrument.name == 'ZTF'))
        if instrument_id is None:
            log("Instrument ZTF not found in the database")
            return
        programid2streamid = make_programid2stream_mapper(session)

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
    
    # Create a Kafka consumer instance with the configuration
    consumer = Consumer(kafka_config)
    # Subscribe to the topic ZTF_alerts_results
    topic_name = params.get("topic", "ZTF_alerts_results")  # Replace with your topic name
    consumer.subscribe([topic_name])  # Subscribe to the topic
    log(f"Subscribed to topic: {topic_name}")
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
                add_thumbnails(record, 'ZTF', session)
            else:
                log(f"Object with id {obj_id} already exists")

            # next we get the candid
            candid = record['candid']
            filter_ids = [f['filter_id'] for f in record['filters']]
            # we checked which candidates are already created
            passed_filter_ids = session.scalars(
                sa.select(
                    Candidate.filter_id,
                    Candidate.filter_id.in_(filter_ids)
                ).where(Candidate.passing_alert_id == candid)
            ).all()
            passed_filter_ids = set(passed_filter_ids)

            for filter_data in record['filters']:
                filt = session.scalar(sa.select(Filter).filter(Filter.id == filter_data['filter_id']))
                if filt is None:
                    log(f"Filter with id {filter_data['filter_id']} does not exist")
                    continue
                if filter_data['filter_id'] not in passed_filter_ids:
                    # create the candidate if it's not already created
                    candidate = Candidate(
                        obj=obj,
                        filter_id=filt.id,
                        # passed_at=filter['passed_at'],
                        # passed at is a timestamp in milliseconds, we need to convert it to a datetime object
                        passed_at=datetime.fromtimestamp(filter_data['passed_at'] / 1000, timezone.utc),
                        passing_alert_id=candid,
                        uploader_id=1
                    )
                    session.add(candidate)
                    log(f"Created candidate with candid {candid}")
                else:
                    log(f"Skipping candidate with candid {candid}")

                # for each filter we get the "annotations" which is a JSON string, we parse it
                annotation_data = json.loads(filter_data['annotations'])
                # let's check if we already have an annotation with the group_id of the filter and the filter name as the origin
                origin = f"{filt.group.name}:{filt.name}"
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
                key = (phot['survey'], phot['programid'])
                if key not in photometry_data:
                    stream_ids = programid2streamid.get(key)
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
                photometry_data[key]['flux'].append(phot['flux'])
                photometry_data[key]['fluxerr'].append(phot['flux_err'])
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