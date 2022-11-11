#!/usr/bin/env python3
# coding=utf-8

import logging
import math
import os
import random
import re
#import simplejson as json
import unicodedata
from datetime import datetime
from random import randint
from typing import Optional
#from unidecode import unidecode

from python.util.cronexpr import CronExpression
from python.util.aws.s3_hook import S3Hook
# from python.util.deng_snowflake import DengSnowflake


class Util:
    """Base class for common utility functions"""

    TMP_DIR = "/var/src/deng-jobs/tmp"
    DATA_TMP_DIR = "/var/src/deng-jobs/data/tmp"
    MAIL_DIR = "/var/src/deng-jobs/data/mail"
    S3_TMP_DIR = "s3://deng-file-store/dev_zone/" + datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    DB_ALIASES = {"dev": "redshift", "prod": "redshift", "object": "s3"}

    def __init__(self):
        self.hostname = os.popen("hostname -s").read().strip()
        self.tmp_dir = Util.TMP_DIR
        self.data_tmp_dir = Util.DATA_TMP_DIR
        self.s3_tmp_dir = Util.S3_TMP_DIR
        self.mail_dir = Util.MAIL_DIR
        # print ('self.tmp_dir = ' + self.tmp_dir)
        if not os.path.exists(self.tmp_dir):
            os.system("mkdir -p " + self.tmp_dir)
        else:
            # cleanup files older than 10 days
            os.system("find " + self.tmp_dir + " -mtime +10 -delete")
        if not os.path.exists(self.data_tmp_dir):
            os.system("mkdir -p " + self.data_tmp_dir)
        else:
            # cleanup files older than 2 days
            os.system("find " + self.data_tmp_dir + " -mtime +2 -delete")

        self.hostname = os.popen("hostname -s").read().strip()
        # mail attributes
        # TO:DO replace email https://sumologic.atlassian.net/servicedesk/customer/portal/5/HELP-17962
        self.admin_email = "bjena@sumologic.com"
        self.from_email = "bjena@sumologic.com"
        self.html_tbl_cellpadding = "5px"
        self.html_tbl_heading_color = "#CCE0FF"
        self.html_tbl_row_color = "#F7F7F7"
        self.mail_track_file = (
            self.tmp_dir + "/mail_track_" + os.popen("date +%Y%m%d").read().strip()
        )
        self.sendmail_program = "/usr/sbin/sendmail"
        self.sendmail_attach_script = (
            os.path.dirname(os.path.realpath(__file__)) + "/sendmail_attachment.sh"
        )

    def ret_rows_html(self, cols, rows, display="horizontal"):
        """ return rows as html formatted """
        html = "<html><head></head><body>"
        if display == "horizontal":
            html += (
                '<table border=0 cellpadding="'
                + self.html_tbl_cellpadding
                + '"><tr bgcolor="'
                + self.html_tbl_heading_color
                + '">'
            )
            for item in cols:
                html += "<td>" + str(item) + "</td>"
            html += "</tr>"
            for row in rows:
                html += '<tr bgcolor="' + self.html_tbl_row_color + '">'
                for item in row:
                    html += "<td>" + str(item) + "</td>"
                html += "</tr>\n"
            html += "</table></body></html>"
        else:
            for row in rows:
                html += '<table border=0 cellpadding="' + self.html_tbl_cellpadding + '">'
                i = 0
                while i < len(cols):
                    html += (
                        '<tr><td bgcolor="' + self.html_tbl_heading_color + '">' + cols[i] + "</td>"
                    )
                    html += (
                        '<td bgcolor="'
                        + self.html_tbl_row_color
                        + '">'
                        + str(row[i])
                        + "</td></tr>\n"
                    )
                    i += 1
                html += "</table><br>"
            html += "</body></html>"
        return html

    def save_to_file(self, contents, filename=None):
        """ save contents to file and return filename """
        if filename is None:  # generate filename
            filename = (
                self.tmp_dir
                + "/tmp_util0_"
                + os.popen("date +%s").read().strip()
                + "_"
                + str(randint(1, 100000))
                + ".txt"
            )
        f = open(filename, "w+")
        f.write(contents)
        f.close()
        return filename

    @staticmethod
    def ret_mins_diff(date1, date2):
        """return difference in minutes between 2 datetimes"""
        fmt = "%Y-%m-%d %H:%M:%S"
        d1 = datetime.strptime(date1, fmt)
        d2 = datetime.strptime(date2, fmt)
        diff = d2 - d1
        diff_minutes = (diff.days * 24 * 60) + (diff.seconds / 60)
        return diff_minutes

    @staticmethod
    def ret_hours_mins_rounded(duration, duration_unit="minute"):
        """ return string in x hours y mins format rounded up"""
        if duration_unit == "second":
            minutes = int(duration) / 60
        elif duration_unit == "hour":
            minutes = int(duration) * 60
        else:
            minutes = duration
        days = 0
        if int(minutes) > 1440:
            days = int(minutes) / 1440
            minutes = math.fmod(int(minutes), 1440)
        hours = int(minutes) / 60
        mins = int(minutes) - (hours * 60)
        if days == 1:
            return "1 day"
        elif days > 1:
            return str(days) + " days"
        elif hours == 1:
            return "1 hr"
        elif hours > 1:
            return str(hours) + " hrs"
        elif mins == 1:
            return "1 min"
        elif mins > 1:
            return str(mins) + " mins"
        else:
            return "<1 min"

    @staticmethod
    def ret_hours_mins(duration, duration_unit="minute"):
        """ return string in x hours y mins format """
        if duration_unit == "second":
            minutes = int(duration) / 60
        elif duration_unit == "hour":
            minutes = int(duration) * 60
        else:
            minutes = duration
        days = 0
        if int(minutes) > 1440:
            days = int(minutes) / 1440
            minutes = math.fmod(int(minutes), 1440)
        hours = int(minutes) / 60
        mins = int(minutes) - (hours * 60)
        if days == 1:
            return "1 day " + str(hours) + " hrs " + str(mins) + " mins"
        elif days > 1:
            return str(days) + " days " + str(hours) + " hrs " + str(mins) + " mins"
        elif hours == 1:
            return "1 hr " + str(mins) + " mins"
        elif hours > 1:
            return str(hours) + " hrs " + str(mins) + " mins"
        elif mins == 1:
            return "1 min"
        elif mins > 1:
            return str(minutes) + " mins"

    @staticmethod
    def chk_execute_now(cron_sched, currtime=None):
        """ check if cron schedule means it is to be executed or not """
        print(cron_sched)
        if currtime is None:
            currtime = datetime.now()
        curr_datetime_tuple = (
            int(currtime.strftime("%Y")),
            int(currtime.strftime("%m")),
            int(currtime.strftime("%d")),
            int(currtime.strftime("%H")),
            int(currtime.strftime("%M")),
        )
        sched = CronExpression(cron_sched)
        execute_flag = sched.check_trigger(curr_datetime_tuple)
        return execute_flag

    @staticmethod
    def ret_emailify(curr_email_list: str, email_ids: str) -> str:
        """Given a string of email IDs update any incomplete email addresses (no @ + domain)
        to be sent to `@sumologic.com` email addresses
        """
        if email_ids is None:
            return ""
        if email_ids.strip() == "":
            return ""
        email_str = email_ids + ","
        email_list = email_str.split(",")
        final_email_str = ""
        for email_token in email_list:
            if email_token in curr_email_list:  # ignore duplicates
                continue
            if "@" in email_token:
                final_email_str = final_email_str + "," + email_token
            elif email_token != "":
                final_email_str = final_email_str + "," + email_token + "@sumologic.com"
        return final_email_str[1:]

    def ret_stroflist(self, lst):
        """ recursive function to stringify list """
        if len(lst) == 0:
            return ""
        else:
            return str(lst[len(lst) - 1]).strip() + "," + str(self.ret_stroflist(lst[:-1]))

    @staticmethod
    def ret_strip_control_characters(input):
        """remove ctrl chars from input (useful for our config where expecting json)"""
        if input:
            # unicode invalid characters
            RE_XML_ILLEGAL = (
                u"([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])"
                + u"|"
                + u"([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])"
                % (
                    chr(0xD800),
                    chr(0xDBFF),
                    chr(0xDC00),
                    chr(0xDFFF),
                    chr(0xD800),
                    chr(0xDBFF),
                    chr(0xDC00),
                    chr(0xDFFF),
                    chr(0xD800),
                    chr(0xDBFF),
                    chr(0xDC00),
                    chr(0xDFFF),
                )
            )
            input = re.sub(RE_XML_ILLEGAL, "", input)
            # ascii control characters
            input = re.sub(r"[\x01-\x1F\x7F]", "", input)
        return input

    @staticmethod
    def random_list(a):
        """ randomize list"""
        b = []
        for i in range(len(a)):
            element = random.choice(a)
            print(element)
            a.remove(element)
            b.append(element)
        return b

    @staticmethod
    def ret_list_of_files(directory):
        """ return list of files in directory """
        files = [os.path.join(directory, fn) for fn in next(os.walk(directory))[2]]
        return files

    @staticmethod
    def ret_replaced_yyyymmdd(unreplaced_str, yyyymmdd, hh=""):
        """perform replacement of all permutations for yyyymmdd"""
        yyyy, mm, dd = yyyymmdd[0:4], yyyymmdd[4:6], yyyymmdd[6:8]
        return (
            unreplaced_str.replace("{{YYYYMMDD}}", yyyymmdd)
            .replace("{{YYYY-MM-DD}}", "%s-%s-%s" % (yyyy, mm, dd))
            .replace("{{YYYY-MM}}", "%s-%s" % (yyyy, mm))
            .replace("{{YYYY}}", yyyy)
            .replace("{{MM-DD}}", "%s-%s" % (mm, dd))
            .replace("{{MM}}", mm)
            .replace("{{DD}}", dd)
            .replace("{{HH}}", hh)
        )

    @staticmethod
    def safe_str(obj):
        """ return the byte string representation of obj """
        try:
            return str(obj)
        except UnicodeEncodeError:
            print("UnicodeEncodeError: String was stripped of unicode characters.")
            return unicodedata.normalize("NFKD", obj)

    @staticmethod
    def asciify(obj):
        """replace common non-ascii characters"""
        obj = unidecode(obj)
        return (
            obj.replace("aEURTM", "'")
            .replace("aEURoe", '"')
            .replace("aEUR", '"')
            .replace("aEUR~", "'")
            .replace("aEUR(tm)", "'")
            .replace("aEUR|", "...")
            .replace('aEUR"', "-")
        )

    @staticmethod
    def escape_quote(obj, purpose=None):
        """escape quotes typically used for sql insertion"""
        if purpose == "postgres":
            return obj.replace("'", "''").replace('"', '\\"').replace("$", "\\$")
        else:
            return obj.replace("'", "\\'")

    @staticmethod
    def remove_newline(obj):
        """remote newlines"""
        return obj.replace("\n\r", " ").replace("\r", " ").replace("\n", " ")

    @staticmethod
    def dos2unix(file):
        """replace Windows '\r' with Linux '\n'"""
        windows = os.popen("cat " + file).read()
        unix = windows.replace("\n\r", "\n").replace("\r", "\n")
        with open(file, "w+") as f:
            f.write(unix)

    @staticmethod
    def pretty(text):
        """pretty print json"""
        text = json.loads(text)
        return json.dumps(text, indent=4, sort_keys=True)

    @staticmethod
    def ret_cleansed_name(name):
        """convert to usable name based on typical rules for table or field names"""
        regex = re.compile("[^a-zA-Z0-9]")
        new_name = regex.sub("_", name.lower())
        # replace protected keywords
        if new_name == "date":  # protected keyword
            return "date_dt"
        else:
            return new_name

    @staticmethod
    def get_logger(name):
        logging.basicConfig(
            format="[%(asctime)s]-[%(name)s:%(lineno)s]-[%(levelname)s] %(message)s"
        )
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        return logger

    @staticmethod
    def read_file(dir_path, file_name):
        with open("{}/{}".format(dir_path, file_name)) as f:
            output = f.read()
        return output


def get_aws_from_file():
    aws_key = ""
    aws_secret = ""
    aws_session_token = ""

    if os.path.exists(os.path.expanduser("~/.aws/credentials")):
        credential_file = os.path.expanduser("~/.aws/credentials")

        with open(credential_file, "r") as f:
            default_found = False
            for c, line in enumerate(f):
                this_line = line.strip()
                if this_line == "[default]":
                    default_found = True
                elif default_found:
                    array = this_line.split(" = ")
                    if array[0].strip() == "aws_access_key_id":
                        aws_key = array[1].strip()
                    elif array[0].strip() == "aws_secret_access_key":
                        aws_secret = array[1].strip()
                    elif array[0].strip() == "aws_session_token":
                        aws_session_token = array[1].strip()

    return aws_key, aws_secret, aws_session_token


def get_s3():
    return S3Hook()


# def get_snowflake(database: Optional[str] = None):
#     return DengSnowflake(
#         user=os.environ.get("SNOWFLAKE_USER"),
#         password=os.environ.get("SNOWFLAKE_PASSWORD"),
#         role=os.environ.get("SNOWFLAKE_ROLE", "AWSADMIN"),
#         database=os.environ.get("SNOWFLAKE_DATABASE", "TEST_DB"),
#         warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "XS_WH"),
#         account=os.environ.get("SNOWFLAKE_ACCOUNT", "SUMOLOGIC"),
#     )