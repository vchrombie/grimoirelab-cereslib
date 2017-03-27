#!/usr/bin/python
# Copyright (C) 2017 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>
#

import pandas

from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search

import datetime

import configparser

import re

import events

import certifi

from enrich import Gender, FileType, EmailFlag, SplitLists, MaxMin, SplitEmail, ToUTF8

from filter import FilterRows


MAPPING_GERRIT = {
"mappings" : {
    "item" : {
        "properties": {
            "date": {
                "type": "date"
            },
            "owner": {
                "index": "not_analyzed",
                "type": "string"
            },
            "id": {
                "type": "string"
            },
            "eventtype": {
                "index": "not_analyzed",
                "type": "string"
            },
            "repository": {
                "index": "not_analyzed",
                "type": "string"
            },
            "projects": {
                "index": "not_analyzed",
                "type": "string"
            }
            }
        }
    }
}
MAPPING_GIT = {
"mappings" : {
    "item" : {
        "properties": {
            "filepath": {
                "index": "not_analyzed",
                "type": "string"
            },
            "date": {
                "type": "date"
            },
            "owner": {
                "index": "not_analyzed",
                "type": "string"
            },
            "committer": {
                "index": "not_analyzed",
                "type": "string"
            },
            "committer_date": {
                "type": "date"
            },
            "user": {
                "index": "not_analyzed",
                "type": "string"
            },
            "email": {
                "index": "not_analyzed",
                "type": "string"
            },
            "repository": {
                "index": "not_analyzed",
                "type": "string"
            },
            "projects": {
                "index": "not_analyzed",
                "type": "string"
            }
        }
    }
}
}

def ESConnection():

    parser = configparser.ConfigParser()
    conf_file = 'settings'
    fd = open(conf_file, 'r')
    parser.readfp(fd)
    fd.close()

    sections = parser.sections()
    for section in sections:
        options = parser.options(section)
        for option in options:
            if option == 'user': user = parser.get(section, option)
            if option == 'password': password = parser.get(section, option)
            if option == 'host': host = parser.get(section, option)
            if option == 'port': port = parser.get(section, option)
            if option == 'path': path = parser.get(section, option)
            if option == 'genderize_key': genderize_key = parser.get(section, option)
            #if option == 'index_gerrit_raw': es_read_index = parser.get(section, option)
            if option == 'index_git_raw': es_read_index = parser.get(section, option)

            if option == 'host_output': host_output = parser.get(section, option)
            if option == 'port_output': port_output = parser.get(section, option)
            if option == 'user_output': user_output = parser.get(section, option)
            if option == 'password_output': password_output = parser.get(section, option)
            if option == 'path_output': path_output = parser.get(section, option)
            #if option == 'index_gerrit_output': es_write_index = parser.get(section, option)
            if option == 'index_git_output': es_write_index = parser.get(section, option)


    connection = "https://" + user + ":" + password + "@" + host + ":" + port + "/"
    print (connection)
    es_read = Elasticsearch([connection], use_ssl=True, verity_certs=True, ca_cert=certifi.where(), scroll='300m')
    #es_read = Elasticsearch(["127.0.0.1:9200"], scroll='20m')

    connection_output = "https://" + user_output + ":" + password_output + "@" + host_output + ":" + port_output + "/" + path_output
    es_write = Elasticsearch([connection_output], use_ssl=True, verity_certs=True, ca_cert=certifi.where(), scroll='300m')

    return es_read, es_write, es_read_index, es_write_index, genderize_key


def openstack_projects():

    import json

    fd = open("openstack.json", "r")
    json_file = fd.read()
    json_data = json.loads(json_file)
    repositories = []
    projects = []
    original_projects = []
    for project in json_data["projects"].keys():
        project_info = json_data["projects"][project]
        for repo in project_info["source_repo"]:
            projects.append(project)
            repositories.append(repo["url"])
            original_projects.append(repo["url"].split("/")[-2] + "/" + repo["url"].split("/")[-1])

    df = pandas.DataFrame()
    df["projects"] = projects
    #df["urls"] = repositories

    # Not used as repositories are url in the enriched indexes
    #df["repository"] = original_projects
    df["repository"] = repositories

    return df

def analyze_gerrit(es_read, es_write, es_read_index, es_write_index, key):

    #TODO
    #projects = openstack_projects()

    es_write.indices.delete(es_write_index, ignore=[400, 404])
    es_write.indices.create(es_write_index, body=MAPPING_GERRIT)

    query = {"query": {"match_all" :{}}}

    items = []
    cont = 1
    uniq_id = 1
    first = True

    for item in helpers.scan(es_read, query, scroll='300m', index=es_read_index):
        items.append(item["_source"])

        if cont % 10000 == 0:
            # Eventizing the first 7500 changesets
            gerrit_events = events.Gerrit(items)
            events_df = gerrit_events.eventize(2)

            print (cont)

            # Adding projects information
            #events_df = pandas.merge(events_df, projects, how='left', on='repository')

            # Adding gender info
            if first:
                enriched_gender = Gender(events_df, key)
                first = False
            else:
                enriched_gender.data = events_df

            events_df = enriched_gender.enrich("owner")
            events_df = events_df.fillna("Unknown")

            # Uploading info to the new ES
            test = events_df.to_dict("index")
            docs = []
            for i in test.keys():
                header = {
                      "_index": es_write_index,
                      "_type": "item",
                      "_id": int(uniq_id),
                      "_source": test[i]
                    }
                docs.append(header)
                uniq_id = uniq_id + 1
            print (len(docs))
            helpers.bulk(es_write, docs)
            items = []
        cont = cont + 1
    helpers.bulk(es_write, docs)


def analyze_git(es_read, es_write, es_read_index, es_write_index, key):

    #projects = openstack_projects()


    es_write.indices.delete(es_write_index, ignore=[400, 404])
    es_write.indices.create(es_write_index, body=MAPPING_GIT)


    s = Search(using=es_read, index=es_read_index)
    s.execute()

    commits = []
    cont = 1
    uniq_id = 1
    first = True


    for item in s.scan():
        commits.append(item.to_dict())

        if cont % 7500 == 0:
            git_events = events.Git(commits)
            events_df = git_events.eventize(2)

            print (cont)
            # Filter information
            data_filtered = FilterRows(events_df)
            events_df = data_filtered.filter_(["filepath"], "-")

            # Add filetype info
            enriched_filetype = FileType(events_df)
            events_df = enriched_filetype.enrich('filepath')

            # Add gender info
            if first:
                enriched_gender = Gender(events_df, key)
                first = False
            else:
                enriched_gender.data = events_df

            events_df = enriched_gender.enrich("owner")
            aux = pandas.DataFrame(events_df["committer"], columns=["committer"])
            enriched_gender.data = aux
            aux = enriched_gender.enrich("committer")

            events_df["committer_gender"] = aux["gender"]
            events_df["committer_gender_probability"] = aux["gender_probability"]
            events_df["committer_gender_analyzed_name"] = aux["gender_analyzed_name"]
            events_df["committer_gender_count"] = aux["gender_count"]

            splitemail = SplitEmail(events_df)
            events_df = splitemail.enrich("owner")

            #events_df = pandas.merge(events_df, projects, how='left', on='repository')

            #all_files = all_files.append(events_df)
            #test = events_df.to_dict("index")
            
            # Deal with surrogates
            convert = ToUTF8(events_df)
            print (events_df.columns)
            events_df = convert.enrich(["gender_analyzed_name", "committer_gender_analyzed_name", "owner", "committer", "user"])
            commits = []
#    max_min = MaxMin(all_files)
#    all_files = max_min.enrich(["date"], "owner")

            test = events_df.to_dict("index")
            docs = []
            for i in test.keys():
                header = {
                      "_index": es_write_index,
                      "_type": "item",
                      "_id": int(uniq_id),
                      "_source": test[i]
                    }
                docs.append(header)
                uniq_id = uniq_id + 1
            helpers.bulk(es_write, docs)
        cont = cont + 1
    helpers.bulk(es_write, docs)

def main():

    es_read, es_write, es_read_index, es_write_index, key = ESConnection()

    #analyze_gerrit(es_read, es_write, es_read_index, es_write_index, key)

    analyze_git(es_read, es_write, es_read_index, es_write_index, key)


if __name__ == "__main__":
    main()
