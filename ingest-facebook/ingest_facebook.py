#!/usr/bin/env python3

import boto3
import csv
import json
import os
import pprint
import sys

from aws_helpers import get_secret
from misc_helpers import flatten_json

from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.business import Business

headers = []
pp = pprint.PrettyPrinter(width=1)


def make_call(time_range, ad_account_id, ad_account_name):
    fields = [
        "account_id",
        "account_name",
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "objective",
        "buying_type",
        "impressions",
        "clicks",
        "inline_post_engagement",
        "spend",
        "reach",
        "cpc",
        "cpm",
        "ctr",
        "actions",
        "video_play_actions",
        "video_p100_watched_actions",
        "video_p75_watched_actions",
        "video_p50_watched_actions",
        "video_p25_watched_actions",
        "date_start",
        "date_stop",
    ]
    params = {
        "level": "ad",
        "time_increment": 1,
        "breakdowns": ["age", "gender"],
        "time_range": {"since": time_range.strftime("%Y-%m-%d"), "until": time_range.strftime("%Y-%m-%d")},
    }

    # get campaigns for Ad Account;
    insights_container = AdAccount(ad_account_id).get_insights(fields=fields, params=params)

    keys = [
        "account_id",
        "account_name",
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "objective",
        "buying_type",
        "age",
        "gender",
        "impressions",
        "inline_post_engagement",
        "clicks",
        "spend",
        "reach",
        "cpc",
        "cpm",
        "ctr",
        "actions_comment",
        "actions_initiate_checkout",
        "actions_landing_page_view",
        "actions_link_click",
        "actions_offsite_conversion_fb_pixel_custom",
        "actions_offsite_conversion_fb_pixel_initiate_checkout",
        "actions_omni_initiated_checkout",
        "actions_onsite_conversion_post_save",
        "actions_page_engagement",
        "actions_post",
        "actions_post_engagement",
        "actions_post_reaction",
        "actions_video_view",
        "video_p100_watched_actions_video_view",
        "video_p25_watched_actions_video_view",
        "video_p50_watched_actions_video_view",
        "video_p75_watched_actions_video_view",
        "video_play_actions_video_view",
        "date_start",
        "date_stop",
    ]
    rows = []
    for insight in insights_container:
        # print(insight)
        py_insight = dict(insight)
        flat_data = flatten_json(py_insight)
        # print(flat_data)
        rows.append(flat_data)
        for x in flat_data:
            if x not in headers:
                headers.append(x)
    if not rows:
        print("NO DATA FOR {} IN: {}".format(ad_account_name, time_range.strftime("%Y-%m-%d")))
        return

    filename = "{}-{}.csv".format(ad_account_id, time_range.strftime("%Y-%m-%d"))
    with open(filename, "w") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=keys, restval="", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    s3.upload_file(filename, os.environ["TARGET_BUCKET"], "raw/facebook/{}".format(filename))
    print("UPLOADED: {}".format(filename))


if __name__ == "__main__":
    # vars;
    s3 = boto3.client("s3")
    yesterday = datetime.utcnow() - timedelta(days=1)
    secrets = json.loads(get_secret(os.environ["AWS_SECRETS"], os.environ["AWS_DEFAULT_REGION"]))
    appId = secrets["facebook_app_id"]
    appSecret = secrets["facebook_app_secret"]
    accessToken = secrets["facebook_access_token"]
    business_id = secrets["facebook_business_id"]

    # auth facebook, init;
    FacebookAdsApi.init(appId, appSecret, accessToken, api_version="v12.0")
    business = Business(fbid=business_id)
    business_accounts = business.get_owned_ad_accounts()

    for act in business_accounts:
        if act["id"] not in ["act_397483258659620"]:
            continue
        account_name = act.api_get(fields=[AdAccount.Field.name])["name"]

        # if automated;
        if os.getenv("IS_AUTOMATED") == "True":
            make_call(yesterday, act["id"], account_name)
        # manual call;
        else:
            startDate = datetime(int(os.environ["START_YEAR"]), int(os.environ["START_MONTH"]), int(os.environ["START_DAY"]))
            endDate = datetime(int(os.environ["END_YEAR"]), int(os.environ["END_MONTH"]), int(os.environ["END_DAY"]))
            datesRange = [startDate + timedelta(days=n) for n in range((endDate - startDate).days + 1)]

            for x in datesRange:
                make_call(x, act["id"], account_name)
    # pp.pprint(sorted(headers))
