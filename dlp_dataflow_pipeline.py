import apache_beam as beam
from apache_beam.io import WriteToText
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
def create_dlp_job(element):
    info_types = ["FIRST_NAME", "EMAIL_ADDRESS", "CREDIT_CARD_NUMBER", "CREDIT_CARD_TRACK_NUMBER",
                  "DATE", "DATE_OF_BIRTH","US_SOCIAL_SECURITY_NUMBER"]
    inspect_config = {"info_types": [{"name": info_type} for info_type in info_types],
                  "include_quote": True}
    project = 'gcp-accelerator-380712'
    print("passed",element)
    storage_config = {
        "cloud_storage_options": {
            "file_set": {
                "url": element
            }
        }
    }
    
    actions = [{"deidentify": {"cloud_storage_output": 'gs://demo_temp_b',
                           "transformation_config": {
                               "deidentify_template": "projects/gcp-accelerator-380712/locations/us-central1/deidentifyTemplates/deidentify",
                           }}}]
    
    job = {
        "inspect_config": inspect_config,
        "storage_config": storage_config,
        "actions": actions
    }
    #topic = google.cloud.pubsub.PublisherClient.topic_path(project, topic_id)
    #parent = f"projects/{project}/locations/us-central1"
    '''
    request = {
        "parent": "projects/gcp-accelerator-380712/locations/us-central1",
        "inspect_job": job
    }
    
    operation = dlp.create_dlp_job(
        request={"parent": "projects/gcp-accelerator-380712/locations/us-central1", "inspect_job": job}
    )
    '''
    from google.cloud import dlp
    dlp_client = dlp.DlpServiceClient()
    operation = dlp_client.create_dlp_job(request={"parent": "projects/gcp-accelerator-380712/locations/us-central1", "inspect_job": job}
    )
    print(f"Inspection operation started: {operation.name}")
    #return operation.name



# Create the Beam pipeline

def main():

    db_args, pipeline_args = get_args()

    #window_interval_sec = float(db_args.window_interval_sec)
    dataflow_gcs_location = db_args.dataflow_gcs_location
    temp_location = dataflow_gcs_location + '/temp'
    job_name = db_args.job_name
    print("job name is: ",job_name)

    beam_options = PipelineOptions(
            pipeline_args,
            runner=db_args.runner,
            project=db_args.project,
            job_name=job_name,
            temp_location=temp_location,
            #streaming=True,
            region=db_args.region,
            #save_main_session=True

        )
    # Define your GCS URLs as input data
    gcs_urls = ['gs://beam_demo_at/hr_records.csv']

    with beam.Pipeline(options=beam_options) as pipeline:
        # Read the input GCS URLs
        urls = pipeline | "CreateURLs" >> beam.Create(gcs_urls)
    
        # Create DLP jobs for each URL
        dlp_jobs = urls | "CreateDLPJobs" >> beam.Map(create_dlp_job)
    
        # Write the job names to a text file
        dlp_jobs | "WriteOutput" >> WriteToText('gs://demo_temp_b/hr_reco.csv')


def get_args():
    parser = argparse.ArgumentParser()
    #parser.add_argument('--window_interval_sec', dest='window_interval_sec', default=300)
    parser.add_argument('--project', dest='project', default='gcp-accelerator-380712')
    parser.add_argument('--region', dest='region', default=' us-central1')
    parser.add_argument('--runner', dest='runner', default='DataflowRunner')
    parser.add_argument('--job_name', dest='job_name', default='pubsub-to-csv')
    parser.add_argument('--dataflow_gcs_location', dest='dataflow_gcs_location', default='gs://temp_bucket_23')


    parsed_db_args, pipeline_args = parser.parse_known_args()

    return parsed_db_args, pipeline_args


if __name__ == "__main__":
    main()

