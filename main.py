import base64
import gzip
import io
import json
import traceback
from datetime import datetime, timedelta, timezone

import fastavro
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import requests
import sqlalchemy as sa
from astropy.io import fits
from astropy.visualization import (
    AsymmetricPercentileInterval,
    ImageNormalize,
    LinearStretch,
    LogStretch,
)
from confluent_kafka import Consumer, KafkaError
from scipy.ndimage import rotate
from sqlalchemy.orm.session import Session

from baselayer.app.env import load_env
from baselayer.app.models import init_db
from baselayer.log import make_log
from skyportal.handlers.api.photometry import add_external_photometry
from skyportal.handlers.api.thumbnail import post_thumbnail
from skyportal.models import (
    Annotation,
    Candidate,
    DBSession,
    Filter,
    Instrument,
    Obj,
    ObjToSuperObj,
    SuperObj,
    Stream,
    User,
)

matplotlib.pyplot.switch_backend("Agg")

env, cfg = load_env()
log = make_log("boom")

init_db(**cfg["database"])

params = cfg.get("services.external.boom.params", {})
kafka_params = params.get("kafka", {})
# fallback to the top-level "boom" config if any
api_params = params.get("api", cfg.get("boom", {}))

thumbnail_types = [
    ("cutoutScience", "new"),
    ("cutoutTemplate", "ref"),
    ("cutoutDifference", "sub"),
]

ZP_PER_SURVEY = {"LSST": 8.9, "ZTF": 23.9}
SNT = 3 # signal-to-noise threshold below which we set the flux to None

# We will populate this with filter ids that come from BOOM, but that are not in the database yet.
# that way we can update filter lists when we encounter new ones from BOOM, but stop doing it for those that
# we know are not in the database (e.g. because they have not been added through this SkyPortal instance)
EXTERNAL_FILTER_IDS = set()


class BoomAPIClient:
    def __init__(
        self, protocol: str, host: str, port: int | None, username: str, password: str
    ):
        self.base_url = f"{protocol}://{host}{f':{port}' if port is not None else ''}"
        self.username = username
        self.password = password
        self.token = None
        self.token_expiry = None

    def get_token(self):
        if (
            self.token is not None
            and self.token_expiry is not None
            and datetime.now(timezone.utc) < self.token_expiry
        ):
            return self.token
        response = requests.post(
            f"{self.base_url}/auth",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "username": self.username,
                "password": self.password,
            },
        )
        response.raise_for_status()
        data = response.json()
        self.token = data["access_token"]
        self.token_expiry = datetime.now(timezone.utc) + timedelta(
            seconds=data["expires_in"]
        )
        return self.token

    def get_cutouts_by_object_id(self, survey: str, object_id: str, retry: bool = True):
        token = self.get_token()
        response = requests.get(
            f"{self.base_url}/surveys/{survey.upper()}/cutouts",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
            params={"objectId": object_id},
        )
        # if the response is a 401, it means the token is invalid, so we refresh the token and try again once
        if retry and response.status_code == 401:
            log("Token expired or invalid, refreshing token")
            self.token = None
            self.get_token()
            return self.get_cutouts_by_object_id(survey, object_id, retry=False)
        response.raise_for_status()
        if "data" not in response.json():
            raise ValueError(f"Unexpected response format: {response.json()}")
        data = response.json()["data"]
        data["objectId"] = object_id
        return data


def get_or_create_object(obj_id, ra, dec, session: Session) -> tuple[Obj, bool]:
    """Get or create an Obj with the given ID. If RA and Dec are provided, they will be used for creation if the object does not exist.

    Parameters
    ----------
    obj_id : str
        The ID of the object to get or create.
    ra : float
        The right ascension of the object, used for creation if the object does not exist.
    dec : float
        The declination of the object, used for creation if the object does not exist.
    session : Session
        The database session to use for the query and potential creation.

    Returns
    -------
    tuple[Obj, bool]
        A tuple containing the Obj instance and a boolean indicating whether the object was created (True) or already existed (False).
    """
    obj = session.scalar(sa.select(Obj).where(Obj.id == obj_id))
    if obj is not None:
        return obj, False

    obj = Obj(
        id=obj_id,
        ra=ra,
        dec=dec,
        ra_dis=ra,
        dec_dis=dec,
    )
    session.add(obj)
    log(f"Created object with id {obj_id}")
    return obj, True


def boom_origin_to_skyportal_origin(boom_origin):
    # Convert the photometry origin from Boom format to SkyPortal format
    # Boom is Alert or ForcedPhot, we want to convert it to None or "alert_fp"
    if boom_origin == "Alert":
        return None
    elif boom_origin == "ForcedPhot":
        return "alert_fp"
    else:
        raise ValueError(f"Unknown Boom photometry origin: {boom_origin}")


def ingest_photometry_array(
    photometry_array,
    obj_id,
    user,
    session,
    programid2streamid,
    survey2instrumentid,
):
    """Ingest photometry data from Boom into SkyPortal.

    Parameters
    ----------
    photometry_array : list[dict]
        A list of photometry points, where each point is a dictionary containing the photometry data in Boom format.
    obj_id : str
        The ID of the object to which the photometry belongs.
    user : User
        The user performing the ingestion, used for attribution in SkyPortal.
    session : Session
        The database session to use for any necessary queries during ingestion.
    programid2streamid : dict[tuple[str, int], list[int]]
        A mapping from (survey, programid) to a list of stream IDs in SkyPortal, used to determine which streams to associate the photometry with based on its survey and programid.
    survey2instrumentid : dict[str, int]
        A mapping from survey names to instrument IDs in SkyPortal. This is used to determine which instrument to associate the photometry with based on its survey.
    """
    # Group points by (survey, programid) and submit each chunk to SkyPortal.
    photometry_data = {}
    for phot in photometry_array:
        try:
            origin = boom_origin_to_skyportal_origin(phot["origin"])
        except ValueError as e:
            log(f"{e}; skipping photometry point for obj_id {obj_id}")
            continue

        if phot["flux"] == -99999.0 or phot["flux_err"] == -99999.0:
            continue
        if phot["flux_err"] is None or not np.isfinite(phot["flux_err"]):
            continue

        survey = str(phot["survey"]).upper()
        key = (survey, phot["programid"])
        if key not in photometry_data:
            stream_ids = programid2streamid.get(key)
            if stream_ids is None:
                log(
                    f"No stream found for survey {survey} and programid {phot['programid']}, skipping photometry"
                )
                continue
            instrument_id = survey2instrumentid.get(survey)
            if instrument_id is None:
                log(f"No instrument found for survey {survey}, skipping photometry")
                continue
            photometry_data[key] = {
                "obj_id": obj_id,
                "group_ids": [1],
                "stream_ids": stream_ids,
                "instrument_id": instrument_id,
                "mjd": [],
                "flux": [],
                "fluxerr": [],
                "filter": [],
                "zp": [],
                "magsys": [],
                "ra": [],
                "dec": [],
                "origin": [],
            }

        photometry_data[key]["mjd"].append(phot["jd"] - 2400000.5)
        flux = phot["flux"]
        if flux is not None and not np.isnan(flux):
            flux = flux * 1e-9  # convert from nJy to Jy
        flux_err = phot["flux_err"] * 1e-9  # convert from nJy to Jy

        # if the signal-to-noise ratio is below the threshold, we set the flux to None
        # to avoid ingesting unreliable photometry shown as detections
        if flux is not None and abs(flux / flux_err) < SNT:
            flux = None
        photometry_data[key]["flux"].append(flux)
        photometry_data[key]["fluxerr"].append(flux_err)
        photometry_data[key]["filter"].append(phot["band"])
        photometry_data[key]["zp"].append(ZP_PER_SURVEY[survey])
        photometry_data[key]["magsys"].append("ab")
        photometry_data[key]["ra"].append(phot["ra"])
        photometry_data[key]["dec"].append(phot["dec"])
        photometry_data[key]["origin"].append(origin)

    for _, data in photometry_data.items():
        add_external_photometry(data, user, session)


def make_thumbnail(
    obj_id, cutout_data, cutout_type: str, thumbnail_type: str, survey: str
):
    rotpa = None
    if survey == "LSST": # LSST uses no compression
        with fits.open(io.BytesIO(cutout_data), ignore_missing_simple=True) as hdu:
            rotpa = hdu[0].header.get("ROTPA", None)
            data = hdu[0].data
    else:
        with (
            gzip.open(io.BytesIO(cutout_data), "rb") as f,
            fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu,
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
    xl = np.greater(np.abs(img), 1e20, out=np.zeros(img.shape, dtype=bool), where=~np.isnan(img))
    if img[xl].any():
        img[xl] = np.nan
    if np.isnan(img).any():
        mean = float(np.nanmean(img.flatten()))
        img = np.nan_to_num(img, nan=mean)

    # Normalize
    stretch = LinearStretch() if cutout_type == "cutoutDifference" else LogStretch()
    norm = ImageNormalize(img, stretch=stretch)
    img_norm = norm(img)

    normalizer = AsymmetricPercentileInterval(lower_percentile=1, upper_percentile=100)
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
                alert["objectId"],
                alert[cutout_type],
                cutout_type,
                thumbnail_type,
                survey
            )
        except Exception as e:
            traceback.print_exc()
            log(f"Failed to create thumbnail for cutout type {cutout_type}: {e}")
            continue
        post_thumbnail(thumbnail, user_id=1, session=session)


def read_avro(msg):
    """
    Reads an Avro file and returns the first record

    Parameters
    ----------
    msg : confluent_kafka.Message
        The Kafka message containing the Avro file in its value

    Returns
    -------
    dict or None
        The first record found in the Avro file, or None if no records are found
    """

    bytes_io = io.BytesIO(msg.value())  # Get the message value as bytes
    bytes_io.seek(0)
    for record in fastavro.reader(bytes_io):
        return record
    return None


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
        if (
            not isinstance(altdata, dict)
            or "collection" not in altdata
            or "selector" not in altdata
        ):
            log(f"Stream with id {stream.id} has invalid altdata, skipping")
            continue
        survey = altdata["collection"].split("_")[0]
        programid = max(altdata["selector"])
        key = (survey, programid)
        if key not in mapper:
            mapper[key] = set()
        mapper[(survey, programid)].add(stream.id)

    # convert from set to list
    for key in mapper:
        mapper[key] = list(mapper[key])
    return mapper


def make_survey2instrumentid(session: Session):
    ztf_instrument_id = session.scalar(
        sa.select(Instrument.id).where(Instrument.name == "ZTF")
    )
    if ztf_instrument_id is None:
        raise ValueError("Instrument ZTF not found in the database")
    lsst_instrument_id = session.scalar(
        sa.select(Instrument.id).where(Instrument.name == "LSST")
    )
    if lsst_instrument_id is None:
        raise ValueError("Instrument LSST not found in the database")
    return {"ZTF": ztf_instrument_id, "LSST": lsst_instrument_id}


def make_boom_filters(session: Session):
    all_filters = session.scalars(sa.select(Filter)).all()
    # only keep Filters where `altdata` has a boom key
    boom_filters: list[Filter] = [
        f for f in all_filters if f.altdata is not None and "boom" in f.altdata
    ]
    boom_filters = {
        f.altdata["boom"]["filter_id"]: {**f.to_dict(), "group": f.group.to_dict()}
        for f in boom_filters
    }
    return boom_filters


def main():
    boom_client = BoomAPIClient(
        protocol=api_params.get("protocol", "https"),
        host=api_params.get("host", "api.kaboom.caltech.edu"),
        port=api_params.get("port", None),
        username=api_params.get("username", ""),
        password=api_params.get("password", ""),
    )

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

        boom_filters = make_boom_filters(session)

    # TODO: validate params
    kafka_config = {
        "bootstrap.servers": f"{kafka_params.get('host', 'localhost')}:{kafka_params.get('port', 9092)}",  # Kafka server and port
        "group.id": kafka_params.get("group_id", "my_group"),  # Consumer group ID
        "auto.offset.reset": "earliest",  # Start reading from the earliest message (DEBUG)
        "enable.auto.commit": False,  # Disable auto-commit of offsets
        "session.timeout.ms": 45000,  # Session timeout for the consumer
        "max.poll.interval.ms": 300000,  # Maximum time between polls
        "security.protocol": "PLAINTEXT",  # Use PLAINTEXT if no authentication
    }

    kafka_username, kafka_password = (
        kafka_params.get("username"),
        kafka_params.get("password"),
    )
    kafka_sasl_mechanism = kafka_params.get("sasl_mechanism", "PLAIN")
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

    log(f"Connecting to Kafka at {kafka_config['bootstrap.servers']} (group ID: {kafka_config['group.id']})")
    consumer = Consumer(kafka_config)
    topic_names = kafka_params.get("topics", ["ZTF_alerts_results", "LSST_alerts_results"])
    consumer.subscribe(topic_names)
    log(f"Subscribed to topics: {topic_names}")
    # Poll for messages from the topic
    is_empty_poll_logged = False
    heartbeat = datetime.now()
    while True:
        if datetime.now() - heartbeat > timedelta(seconds=60):
            heartbeat = datetime.now()
            log("Boom listener heartbeat.")

        msg = consumer.poll(timeout=30.0)
        if msg is None:
            if not is_empty_poll_logged: # only log the first time we get an empty poll to avoid spamming the logs
                log("No message received within the timeout period.")
                is_empty_poll_logged = True
            continue
        if msg.error():
            # Handle any errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log("End of partition reached.")
                continue
            else:
                log(f"Error: {msg.error()}")
                continue
        is_empty_poll_logged = False # reset the flag when we successfully receive a message

        # Successfully received a message
        record = read_avro(msg)

        if record is None:
            log("No record found in the Avro message")
            continue

        with DBSession() as session:
            obj_id = record["objectId"]
            survey = record["survey"].upper()
            obj, obj_created = get_or_create_object(
                obj_id,
                record["ra"],
                record["dec"],
                session
            )
            if obj_created:
                add_thumbnails(record, survey, session)
            else:
                log(f"Object with id {obj_id} already exists")

            # we checked which candidates are already created
            candid = record["candid"]
            passed_filter_ids = session.scalars(
                sa.select(
                    Candidate.filter_id,
                ).where(Candidate.passing_alert_id == candid)
            ).all()
            passed_filter_ids = set(passed_filter_ids)

            created_candidates = False
            for filter_data in record["filters"]:
                filt = boom_filters.get(filter_data["filter_id"])
                if filt is None:
                    # if filter_data["filter_id"] is in the EXTERNAL_FILTER_IDS set, it means
                    # we know this filter is not in the database, so we skip it without logging
                    if filter_data["filter_id"] in EXTERNAL_FILTER_IDS:
                        continue
                    # else we try to refresh the filter list from the database, in case
                    # new filters have been added since the start of the program
                    boom_filters = make_boom_filters(session)
                    filt = boom_filters.get(filter_data["filter_id"])
                    if filt is None:
                        log(
                            f"Filter with id {filter_data['filter_id']} does not exist in SkyPortal"
                        )
                        EXTERNAL_FILTER_IDS.add(filter_data["filter_id"])
                        continue
                if filt["id"] in passed_filter_ids:
                    continue

                # create the candidate if it has not been created yet for this filter and this candid
                try:
                    with session.begin_nested():
                        candidate = Candidate(
                            obj=obj,
                            filter_id=filt["id"],
                            # convert passed_at from a timestamp in milliseconds to a datetime object
                            passed_at=datetime.fromtimestamp(
                                filter_data["passed_at"] / 1000, timezone.utc
                            ),
                            passing_alert_id=candid,
                            uploader_id=1
                        )
                        session.add(candidate)
                except Exception as e:
                    log(f"Error creating candidate with candid {candid} and filter {filt['id']}: {e}")
                    continue # If the candidate is not created successfully, we skip the annotation creation

                created_candidates = True
                log(f"Created candidate with candid {candid}")
                try:
                    with session.begin_nested():
                        annotation_data = json.loads(filter_data["annotations"])

                        group_name = filt["group"].get("nickname")
                        if group_name is None: # if nickname is not present, use the name
                            group_name = filt["group"]["name"]
                        origin = f"{group_name}:{filt['name']}"

                        existing_annotation = session.scalar(
                            sa.select(Annotation).filter(
                                Annotation.obj_id == obj_id, Annotation.origin == origin
                            )
                        )
                        if existing_annotation is None:
                            annotation = Annotation(
                                obj=obj, data=annotation_data, origin=origin, author_id=1
                            )
                            session.add(annotation)
                            log(f"Created annotation with origin {origin}")
                        else:
                            # we update the data of the annotation
                            existing_annotation.data = annotation_data
                            log(f"Updated annotation with origin {origin}")
                except Exception as e:
                    log(f"Error processing annotation for object {obj_id} and filter {filt['id']}: {e}")

            if not created_candidates:
                log(f"No new candidates created for object {obj_id} with candid {candid}")
                continue

            session.commit()

            ingest_photometry_array(
                record.get("photometry", []),
                obj_id,
                user,
                session,
                programid2streamid,
                survey2instrumentid,
            )

            session.commit()

            # Ingest cross-survey matches (object + photometry), if provided.
            survey_matches: dict[str, dict] = record.get("survey_matches", {})
            associated_with = set()
            if isinstance(survey_matches, dict):
                for match_survey, match in survey_matches.items():
                    match_survey = match_survey.upper()
                    # we never have matches with the same survey as the main object,
                    # so let's skip those just in case
                    if match is None or match_survey == survey or not isinstance(match, dict):
                        continue

                    match_obj_id = match["objectId"]

                    _, match_obj_created = get_or_create_object(
                        match_obj_id,
                        match["ra"],
                        match["dec"],
                        session,
                    )

                    # TODO: grab the cutouts for the match object (if newly added) from the BOOM API
                    if match_obj_created:
                        try:
                            cutouts = boom_client.get_cutouts_by_object_id(
                                match_survey, match_obj_id
                            )
                            add_thumbnails(cutouts, match_survey, session)
                        except Exception as e:
                            log(
                                f"Failed to get cutouts for match object {match_obj_id} from survey {match_survey}: {e}"
                            )

                    ingest_photometry_array(
                        match["photometry"],
                        match_obj_id,
                        user,
                        session,
                        programid2streamid,
                        survey2instrumentid,
                    )

                    associated_with.add(match_obj_id)

            if associated_with:
                # first we check if this object is already part of a super object
                super_obj = session.scalar(
                    sa.select(SuperObj).join(ObjToSuperObj).where(ObjToSuperObj.obj_id == obj_id)
                )
                if super_obj is None:
                    # if not, we create a new super object and associate the main object and the matches with it
                    super_obj = SuperObj()
                    session.add(super_obj)
                    session.flush()  # flush to get the super_obj.id

                    obj_to_superobj = ObjToSuperObj(obj_id=obj_id, super_obj_id=super_obj.id)
                    session.add(obj_to_superobj)

                    for match_obj_id in associated_with:
                        match_obj_to_superobj = ObjToSuperObj(
                            obj_id=match_obj_id, super_obj_id=super_obj.id
                        )
                        session.add(match_obj_to_superobj)

                    log(f"Created super object with id {super_obj.id} and associated {obj_id} with matches: {', '.join(sorted(associated_with))}")
                else:
                    # if the super object already exists, we just need to associate the matches with it (the main object is already associated)
                    existing_associations_obj_ids = session.scalars(
                        sa.select(ObjToSuperObj.obj_id).where(
                            ObjToSuperObj.super_obj_id == super_obj.id,
                        )
                    ).all()
                    new_associated_obj_ids = associated_with - set(existing_associations_obj_ids)
                    for match_obj_id in new_associated_obj_ids:
                        match_obj_to_superobj = ObjToSuperObj(
                            obj_id=match_obj_id, super_obj_id=super_obj.id
                        )
                        session.add(match_obj_to_superobj)
                    if new_associated_obj_ids:
                        log(f"Updated super object with id {super_obj.id} to associate {obj_id} with new matches: {', '.join(sorted(new_associated_obj_ids))}")
            session.commit()


if __name__ == "__main__":
    main()
