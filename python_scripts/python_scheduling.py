#!/usr/bin/env python

"""python_scheduling.py: Use python scheduling to run jobs at regular intervals."""

"""
I had a situation where our legacy scheduling platform Talend would have license expiry coming up, while the new
scheduling in Airflow in the new platform wasn't quite ready to go. There was a period of about 3 months during
which the jobs on the legacy platform had to continue to run. To bridge the scheduling gap, I used Python scheduling
for the time being.

This demonstrates a number of Python features I used:
- Execution of jobs from a continuously running scheduler (should be run in background)
  - Config file driven - allows dynamically changing parameters that may need changing without stopping the scheduler
  - Threaded execution to ensure scheduler does not error out if any of the jobs fail
  - Passing parameters to threaded jobs and scheduled jobs
- Sending HTML alert emails. For this example I have used Gmail using an app password that has to be set up separately
  - See https://support.google.com/accounts/answer/185833?hl=en for setting up app password
  - In a corporate setting a password usually won't be required
- Usage of AWS Secrets Manager to retrieve app password instead of putting it in config file. In a corporate
  environment, this could be for database credentials etc.
- Practical use of Python eval to read in a function name from config file and execute that function

Python scheduling is not meant to replace an actual scheduler as it lacks functionality like
history of job runs etc., but for an intermediate period it is more than adequate, especially if properly parameterized.
This script demonstrates the principle by sending HTML emails rather than doing any actual database activity which
would typically be what would need to be done in Production.

Multiple jobs can be scheduled using this script, based on an YAML config file.

Parameters can be passed via the YAML config file. However, things like database credentials should not be put directly
in the file. It could contain a secret name (e.g. for AWS Secrets Manager or Databricks Secrets utility), which in turn
should be used by the job (which in this case is defined as a function) to obtain the credentials.

Sample config file sched_config.yml in https://github.com/arindamsinha12/scripts/tree/main/config is used for
this example.
"""

import schedule
import threading
import yaml
import smtplib
import email
import json
import boto3
import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from botocore.exceptions import ClientError
from time import sleep

__author__ = "Arindam Sinha"
__license__ = "GPL"
__version__ = "1.0.0"
__status__ = "Prototype"


# Send an HTML email
def send_email(eml_sndr, eml_rcvrs, password, subject, message_body):
    port = 587
    smtp_server = "smtp.gmail.com"

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = eml_sndr
    message["To"] = eml_rcvrs

    body = MIMEText(message_body, "html")
    message.attach(body)

    server = smtplib.SMTP(smtp_server, 587)
    server.connect(smtp_server, 587)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(eml_sndr, password)
    text = message.as_string()
    server.sendmail(eml_sndr, eml_rcvrs, text)
    server.quit()


# Retrieve any credentials for AWS Secrets Manager. Any Secret Management utility can be used
def get_email_password(secret_name):
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Convert the response string to a dictionary and return just the password value
    return json.loads(get_secret_value_response['SecretString'])['password']


# Job to be run. This is where the main functionality should reside.
def job_a(fnm, lnm, eml_sndr, eml_rcvrs, password):
    print(f'Hi, {fnm} {lnm}, Job A running...')

    subject = 'Job A is Running'

    # Create HTML email body
    email_body = f"""\
    <html>
      <body>
        <p>Hi {fnm} {lnm},<br>
          Job A is running...</p>
      </body>
    </html>
    """

    send_email(eml_sndr, eml_rcvrs, password, subject, email_body)


# Another job to be run
def job_b(fnm, lnm, eml_sndr, eml_rcvrs, password):
    print(f'Hi, {fnm} {lnm}, Job B running...')

    subject = 'Job B is Running'

    # Create HTML email body
    email_body = f"""\
    <html>
      <body>
        <p>Hi {fnm} {lnm},<br>
          Job B is running...</p>
      </body>
    </html>
    """

    send_email(eml_sndr, eml_rcvrs, password, subject, email_body)


# Run jobs threaded, with arguments passed. Protects the scheduler process for error on failure of a job
def run_thrd(job, args):
    job_thread = threading.Thread(target=job, args=args)
    job_thread.start()


if __name__ == "__main__":

    yaml_file_path = '/tmp/sched_config.yml'  # Your YAML config file
    prev_run_time = {}
    init_run = True

    email_sender = email_receivers = email_password = None
    scheduler_stop_flag = False

    try:
        while True:
            # Read the config file every time the loop runs
            with open(yaml_file_path, 'r') as file:
                sched_config = yaml.safe_load(file)

            if init_run:
                email_password = get_email_password(sched_config['common']['email_secret_name'])
                email_sender = sched_config['common']['email_sender']
                init_run = False

            # If stop_scheduler flag is set to true, exit gracefully. This is a continuously running scheduler
            if sched_config['common']['stop_scheduler']:
                scheduler_stop_flag = True
                # Reset the stop_scheduler flag before exiting
                sched_config['common']['stop_scheduler'] = False
                with open(yaml_file_path, 'w') as yaml_file:
                    yaml.dump(sched_config, yaml_file, indent=4, sort_keys=False)

                break

            # If run time for jobs has changed, clear and recreate the schedule. Rerun immediately any job flagged
            # for rerun
            run_time_changed = False
            job_run_immed = False
            for (k, v) in sched_config.items():
                # Do this for all jobs. Exclude 'common' as that is for parameters common to all jobs
                if k != 'common':
                    if v['run_time'] != prev_run_time.get(k, '99:99'):
                        run_time_changed = True
                        prev_run_time[k] = v['run_time']

                    # Run any individual job immediately if flagged. Useful for rerunning failed jobs.
                    if v['run_immediately']:
                        email_receivers = v['email_receivers']

                        # Use eval to run the job after reading it from config file. New jobs can be
                        # easily added to file without stopping the scheduler
                        run_thrd(eval(k), tuple(v['arguments'].split(',')) +
                                 (email_sender, email_receivers, email_password))
                        v['run_immediately'] = False
                        job_run_immed = True

            # Reset any job specific run_immediately flags
            if job_run_immed:
                with open(yaml_file_path, 'w') as yaml_file:
                    yaml.dump(sched_config, yaml_file, indent=4, sort_keys=False)

            # Recreate schedule if job run time(s) have changed
            if run_time_changed:
                schedule.clear()
                for (k, v) in sched_config.items():
                    if k != 'common':
                        email_receivers = v['email_receivers']
                        schedule.every().day.at(v['run_time']).do(run_thrd,
                                                                  eval(k), tuple(v['arguments'].split(',')) +
                                                                  (email_sender, email_receivers, email_password))
                        prev_run_time[k] = v['run_time']

            # Immediately run all scheduled jobs if flagged, and reset the run_all_immediately flag
            if sched_config['common']['run_all_immediately']:
                sched_config['common']['run_all_immediately'] = False
                with open(yaml_file_path, 'w') as yaml_file:
                    yaml.dump(sched_config, yaml_file, indent=4, sort_keys=False)

                schedule.run_all()

            # Run any scheduled jobs if the run time for the job has been reached
            schedule.run_pending()

            # Wait for the configured number of seconds before next loop
            sleep(sched_config['common']['sleep_duration'])
    except Exception as ex:
        print('Error:', ex)

    if scheduler_stop_flag:
        print('Exiting based on stop_scheduler flag.')

    exit(0)
