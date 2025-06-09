import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
import os
from dotenv import load_dotenv
from datetime import datetime

import logging

# Get logger for this module
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

def create_email_transporter():
    """
    Creates and returns an SMTP connection.
    """
    # Get email config from environment variable and split by ':'
    email_config = os.getenv('EMAIL_CONFIG', '').split(':')
    if len(email_config) != 2:
        raise ValueError("EMAIL_CONFIG environment variable must be in format 'user:password'")
    
    email_user, email_password = email_config
    
    # Create SMTP connection
    smtp_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    smtp_server.login(email_user, email_password)
    return smtp_server

async def send_email(from_shop: str, to_email: str, body: str, subject: str) -> bool:
    """
    Sends an email using SMTP.
    
    Args:
        from_shop (str): The shop name (will be formatted)
        to_email (str): Recipient email address
        body (str): HTML body of the email
        subject (str): Email subject
        
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    try:
        # Format the from name by removing .myshopify.com
        from_name = from_shop.replace('.myshopify.com', '')
        
        # Create message
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = f'"{from_name} (Dyno)" <info@bylobyte.com>'
        msg['To'] = to_email
        
        # Attach HTML body
        msg.attach(MIMEText(body, 'html'))
        
        logger.info(f"Attempting to send email to: {to_email}")
        
        # Create SMTP connection and send
        with create_email_transporter() as smtp:
            smtp.send_message(msg)
            logger.info('Email sent successfully!')
            return True
            
    except Exception as error:
        logger.error(f'Error sending email: {str(error)}')
        logger.error(f'Error type: {type(error).__name__}')
        logger.error(f'Error details: {error.__dict__ if hasattr(error, "__dict__") else "No additional details"}')
        return False

async def send_codes_generated_email(shop_data: dict) -> bool:
    """
    Sends an email notification about generated discount codes.
    
    Args:
        shop_data (dict): Dictionary containing shop information including email
        
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    try:
        if not isinstance(shop_data, dict):
            print(f"Invalid shop_data type: {type(shop_data)}. Expected dict.")
            return False
            
        if not shop_data:
            print("Empty shop_data dictionary")
            return False
            
        shop_name = shop_data.get('shop')
        to_email = shop_data.get('email')
        
        if not shop_name:
            print("No shop name found in shop data")
            return False
            
        if not to_email:
            print("No email address found in shop data")
            return False
            
        # Format the shop name by removing .myshopify.com
        formatted_shop_name = shop_name.replace('.myshopify.com', '')
        
        # HTML email template
        email_body = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Discount Codes Generated</title>
    <style>
        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            font-size: 16px;
            line-height: 1.6;
            margin: 0;
            padding: 0;
            background-color: #ffffff;
        }}
        .container {{
            max-width: 500px;
            margin: 20px auto;
            font-size: 15px;
            color: #3c3c3c;
            padding: 30px;
            border: 1px solid rgb(201, 201, 201);
            border-radius: 8px;
            background-color: #ffffff;
            box-sizing: border-box;
            box-shadow: -3px 3px 2px 1px #c8dafb;
        }}
        .letter-content {{
            margin-bottom: 30px;
        }}
        .signature {{
            border-top: 1px solid #e0e0e0;
            padding-top: 20px;
            margin-top: 30px;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        .signature img {{
            width: 46px;
            height: 46px;
            margin-right: 10px;
        }}
        .date {{
            text-align: right;
            margin-bottom: 20px;
            color: #777;
            font-size: 14px;
        }}
        .greeting {{
            margin-bottom: 20px;
        }}
        .footer {{
            max-width: 600px;
            margin: 10px auto;
            text-align: center;
            font-size: 12px;
            color: #888888;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="date">
            {datetime.now().strftime('%B %d, %Y')}
        </div>
        
        <div class="greeting">
            Dear {formatted_shop_name} Team,
        </div>
        
        <div class="letter-content">
            <p>We're pleased to inform you that your bulk discount codes have been successfully generated.</p>
            
            <p>Go to Dyno's dashboard to download the codes.</p>
            
            <p>Thank you for using our Dyno Bulk Discount Codes service.</p>
        </div>
        
        <div class="signature">
            <img src="https://i.imgur.com/ATkDFI1.png" alt="Dyno Icon">
            <div>
                <div style="font-size: 18px; color: #333333; font-weight: 500;">Dyno Bulk Discount Codes</div>
                <div style="font-size: 13px; color: #666;">5 Charlesbank Rd, Newton, MA, 02458</div>
            </div>
        </div>
    </div>
    
    <div class="footer">
        <p>This is an automated message. Please do not reply to this email.</p>
    </div>
</body>
</html>
        """
        
        # Send the email using the existing send_email function
        return await send_email(
            from_shop=shop_name,
            to_email=to_email,
            body=email_body,
            subject="Discount codes are generated"
        )
        
    except Exception as error:
        print(f"Error sending discount codes email: {error}")
        return False

