#!/usr/bin/env python3
import smtplib
import argparse
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue
import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

print("""
---------------------------
AWS SMTP Bulk Email Sender 
---------------------------
""")

# Load environment variables from .env file
load_dotenv()

class SESStatusChecker:
    def __init__(self, region):
        self.ses_client = boto3.client('ses', region_name=region)
        
    def check_sending_enabled(self):
        try:
            response = self.ses_client.get_account_sending_enabled()
            return response.get('Enabled', False)
        except ClientError as e:
            print(f"Error checking SES sending status: {e}")
            return False

class SMTPRotator:
    def __init__(self, smtp_file):
        self.smtp_servers = []
        self.current_index = 0
        self.lock = threading.Lock()
        self.load_smtp_servers(smtp_file)
        # Use the region from the first SMTP server for SES client
        self.status_checker = SESStatusChecker(self.smtp_servers[0]['region']) if self.smtp_servers else None
        
    def load_smtp_servers(self, smtp_file):
        try:
            with open(smtp_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        parts = line.split('|')
                        if len(parts) >= 5:  # Now expecting 5 parts including region
                            server, port, username, password, region = parts[:5]
                            self.smtp_servers.append({
                                'server': server,
                                'port': int(port),
                                'username': username,
                                'password': password,
                                'region': region,
                                'count': 0,
                                'disabled': False
                            })
            if not self.smtp_servers:
                raise ValueError("No valid SMTP servers found in the file")
        except Exception as e:
            print(f"Error loading SMTP servers: {str(e)}")
            exit(1)

    def get_next_server(self):
        with self.lock:
            # First check SES sending status
            if not self.status_checker.check_sending_enabled():
                print("\nERROR: AWS SES sending is currently paused for your account.")
                print("Please check your AWS SES dashboard and verify your sending limits.")
                exit(1)
                
            # Find the next available server with count < 3 and not disabled
            for _ in range(len(self.smtp_servers)):
                server = self.smtp_servers[self.current_index]
                
                if server['disabled']:
                    self.current_index = (self.current_index + 1) % len(self.smtp_servers)
                    continue
                    
                if server['count'] < 3:
                    server['count'] += 1
                    return server
                    
                self.current_index = (self.current_index + 1) % len(self.smtp_servers)
                server['count'] = 0  # Reset counter if we've tried all servers
                
            # If all servers have count >= 3, wait and try again
            time.sleep(1)
            return self.get_next_server()

    def disable_server(self, server_info):
        with self.lock:
            for server in self.smtp_servers:
                if (server['server'] == server_info['server'] and 
                    server['port'] == server_info['port'] and
                    server['username'] == server_info['username'] and
                    server['region'] == server_info['region']):
                    server['disabled'] = True
                    print(f"\nDisabled server: {server['server']} due to sending pause")

class EmailSender:
    def __init__(self, smtp_rotator, email_list, subject, message, mail_from, html_message=None):
        self.smtp_rotator = smtp_rotator
        self.email_list = email_list
        self.subject = subject
        self.message = message
        self.mail_from = mail_from
        self.html_message = html_message
        self.success_count = 0
        self.failure_count = 0
        self.lock = threading.Lock()
        self.progress_queue = Queue()
        
    def send_email(self, recipient):
        smtp_info = self.smtp_rotator.get_next_server()
        msg = MIMEMultipart('alternative')
        msg['From'] = self.mail_from
        msg['To'] = recipient
        msg['Subject'] = self.subject
        
        # Attach both plain text and HTML versions
        part1 = MIMEText(self.message, 'plain')
        msg.attach(part1)
        
        if self.html_message:
            part2 = MIMEText(self.html_message, 'html')
            msg.attach(part2)
        
        try:
            with smtplib.SMTP(smtp_info['server'], smtp_info['port']) as server:
                server.starttls()
                server.login(smtp_info['username'], smtp_info['password'])
                server.sendmail(self.mail_from, recipient, msg.as_string())
            
            with self.lock:
                self.success_count += 1
            self.progress_queue.put((recipient, True, None))
        except smtplib.SMTPResponseException as e:
            if e.smtp_code == 454 and "Sending paused for this account" in str(e):
                self.smtp_rotator.disable_server(smtp_info)
            with self.lock:
                self.failure_count += 1
            self.progress_queue.put((recipient, False, str(e)))
        except Exception as e:
            with self.lock:
                self.failure_count += 1
            self.progress_queue.put((recipient, False, str(e)))
    
    def display_progress(self, total):
        while self.success_count + self.failure_count < total:
            time.sleep(0.5)
            print(f"\rProgress: {self.success_count + self.failure_count}/{total} | Success: {self.success_count} | Failed: {self.failure_count}", end='')
        print()  # New line after completion

def load_email_list(email_file):
    try:
        with open(email_file, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error loading email list: {str(e)}")
        exit(1)

def get_config_from_env():
    """Get configuration from environment variables with fallback to .env file"""
    config = {
        'smtp_file': os.getenv('SMTP_FILE'),
        'email_list': os.getenv('EMAIL_LIST'),
        'subject': os.getenv('SUBJECT'),
        'message_file': os.getenv('MESSAGE_FILE'),
        'html_message_file': os.getenv('HTML_MESSAGE_FILE'),
        'threads': os.getenv('THREADS', '5'),
        'mail_from': os.getenv('MAIL_FROM')
    }
    
    # Validate required configurations
    required = ['smtp_file', 'email_list', 'subject', 'message_file', 'mail_from']
    for key in required:
        if not config[key]:
            print(f"Error: {key.upper()} is not set in .env file or environment variables")
            exit(1)
            
    # Convert threads to integer
    try:
        config['threads'] = int(config['threads'])
    except ValueError:
        print("Error: THREADS must be an integer")
        exit(1)
        
    return config

def main():
    # Get configuration from command line or .env file
    parser = argparse.ArgumentParser(description='AWS SMTP Bulk Email Sender with SES Status Check')
    parser.add_argument('--smtp-file', help='File containing SMTP servers (format: server|port|username|password|region)')
    parser.add_argument('--email-list', help='File containing list of email recipients (one per line)')
    parser.add_argument('--subject', help='Email subject')
    parser.add_argument('--message-file', help='File containing plain text email message')
    parser.add_argument('--html-message-file', help='Optional file containing HTML email message')
    parser.add_argument('--threads', type=int, help='Number of concurrent threads')
    parser.add_argument('--mail-from', help='Email address to send from (must be verified in AWS SES)')
    
    args = parser.parse_args()
    
    # Get config from .env if not provided via command line
    env_config = get_config_from_env()
    
    # Use command line arguments if provided, otherwise use .env config
    smtp_file = args.smtp_file or env_config['smtp_file']
    email_list_file = args.email_list or env_config['email_list']
    subject = args.subject or env_config['subject']
    message_file = args.message_file or env_config['message_file']
    html_message_file = args.html_message_file or env_config['html_message_file']
    threads = args.threads or env_config['threads']
    mail_from = args.mail_from or env_config['mail_from']
    
    # Load messages
    try:
        with open(message_file, 'r') as f:
            message = f.read()
    except Exception as e:
        print(f"Error loading message file: {str(e)}")
        exit(1)
    
    html_message = None
    if html_message_file:
        try:
            with open(html_message_file, 'r') as f:
                html_message = f.read()
        except Exception as e:
            print(f"Error loading HTML message file: {str(e)}")
            exit(1)
    
    # Load email list
    email_list = load_email_list(email_list_file)
    total_emails = len(email_list)
    
    # Initialize SMTP rotator
    smtp_rotator = SMTPRotator(smtp_file)
    
    # Get AWS region from the first SMTP server
    aws_region = smtp_rotator.smtp_servers[0]['region'] if smtp_rotator.smtp_servers else 'us-east-1'
    
    print(f"""
Configuration Summary:
- AWS Region: {aws_region} (from SMTP configuration)
- SMTP Servers: {len(smtp_rotator.smtp_servers)}
- Recipients: {total_emails}
- Subject: {subject}
- From Address: {mail_from}
- Threads: {threads}
- Message File: {message_file}
- HTML Message File: {html_message_file or 'None'}
""")
    
    # Initialize email sender
    email_sender = EmailSender(smtp_rotator, email_list, subject, message, mail_from, html_message)
    
    # Start progress display thread
    progress_thread = threading.Thread(target=email_sender.display_progress, args=(total_emails,))
    progress_thread.daemon = True
    progress_thread.start()
    
    # Start sending emails
    start_time = time.time()
    print("Starting email sending process...\n")
    
    with ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(email_sender.send_email, email_list)
    
    # Wait for progress thread to finish
    while email_sender.success_count + email_sender.failure_count < total_emails:
        time.sleep(0.5)
    
    # Print summary
    print("\nEmail sending completed!")
    print(f"Total time: {time.time() - start_time:.2f} seconds")
    print(f"Successfully sent: {email_sender.success_count}")
    print(f"Failed to send: {email_sender.failure_count}")
    
    # Print failures if any
    if email_sender.failure_count > 0:
        print("\nFailed deliveries:")
        while not email_sender.progress_queue.empty():
            recipient, success, error = email_sender.progress_queue.get()
            if not success:
                print(f"{recipient}: {error}")

if __name__ == '__main__':
    main()