from datetime import timedelta, datetime
from typing import List, Dict, Optional


def get_default_dag_args(notification_emails: Optional[List[str]]) -> Dict[str, any]:
    args = {
        "depends_on_past": False,
        "start_date": datetime.strptime('2022-07-17', '%Y-%m-%d'),
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2)
    }

    if notification_emails is not None:
        args['email'] = [email.strip() for email in notification_emails]

    return args
