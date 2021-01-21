# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>
#

import configparser

import certifi
import pandas
from cereslib.filter import FilterRows
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search

from cereslib.enrich import Gender, FileType, SplitEmail, ToUTF8, Uuid
from perceval.backends.core.git import Git, Gerrit

MAPPING_GERRIT = {
    "mappings": {
        "item": {
            "properties": {
                "date": {
                    "type": "date"
                },
                "owner": {
                    "type": "keyword"
                },
                "id": {
                    "type": "keyword"
                },
                "eventtype": {
                    "type": "keyword"
                },
                "repository": {
                    "type": "keyword"
                },
                "projects": {
                    "type": "keyword"
                },
                "gender": {
                    "type": "keyword"
                },
                "uuid": {
                    "type": "keyword"
                }
            }
        }
    }
}
MAPPING_GIT = {
    "mappings": {
        "item": {
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
                },
                "gender": {
                    "type": "keyword"
                },
                "uuid": {
                    "type": "keyword"
                }
            }
        }
    }
}


def ESConnection():
    parser = configparser.ConfigParser()
    conf_file = 'settings'
    fd = open(conf_file, 'r')
    parser.read_file(fd)
    fd.close()

    sections = parser.sections()
    for section in sections:
        options = parser.options(section)
        for option in options:
            if option == 'user':
                user = parser.get(section, option)
            if option == 'password':
                password = parser.get(section, option)
            if option == 'host':
                host = parser.get(section, option)
            if option == 'port':
                port = parser.get(section, option)
            if option == 'path':
                path = parser.get(section, option)
            if option == 'genderize_key':
                genderize_key = parser.get(section, option)
            if option == 'index_gerrit_raw':
                es_read_gerrit_index = parser.get(section, option)
            if option == 'index_git_raw':
                es_read_git_index = parser.get(section, option)

            if option == 'host_output':
                host_output = parser.get(section, option)
            if option == 'port_output':
                port_output = parser.get(section, option)
            if option == 'user_output':
                user_output = parser.get(section, option)
            if option == 'password_output':
                password_output = parser.get(section, option)
            if option == 'path_output':
                path_output = parser.get(section, option)
            if option == 'index_gerrit_output':
                es_write_gerrit_index = parser.get(section, option)
            if option == 'index_git_output':
                es_write_git_index = parser.get(section, option)

    connection = "https://" + user + ":" + password + "@" + host + ":" + port + "/"
    print(connection)
    es_read = Elasticsearch([connection], use_ssl=True, verity_certs=True, ca_cert=certifi.where(), scroll='300m',
                            timeout=100)

    connection_output = "https://" + user_output + ":" + password_output + "@" + host_output + ":" + port_output + \
                        "/" + path_output
    es_write = Elasticsearch([connection_output], use_ssl=True, verity_certs=True, ca_cert=certifi.where(),
                             scroll='300m', timeout=100)

    return es_read, es_write, es_read_git_index, es_read_gerrit_index, es_write_git_index, \
        es_write_gerrit_index, genderize_key


def openstack_projects():
    import yaml

    fd = open("openstack_projects.yaml", "r")
    yaml_text = fd.read()
    yaml_data = yaml.load(yaml_text)

    urls = []
    repos = []
    projects = []

    for project in yaml_data:
        deliverables = yaml_data[project]["deliverables"]

        for subproject in deliverables.keys():
            repositories = deliverables[subproject]['repos']
            for repo in repositories:
                projects.append(project)
                repos.append(repo)
                urls.append("git://git.openstack.org/" + repo)
    df = pandas.DataFrame()
    df["projects"] = projects
    df["repository"] = repos
    df["urls"] = urls

    return df


def analyze_gerrit(es_read, es_write, es_read_index, es_write_index, key):
    # Retrieve projects info
    projects = openstack_projects()

    # Retrieve uuids info
    uuids = Uuid(pandas.DataFrame(), file_path='openstack_uuids.csv')

    # Retrieve gender cached data
    enriched_gender = Gender(pandas.DataFrame(), key, "gerrit_gender.csv")

    es_write.indices.delete(es_write_index, ignore=[400, 404])
    es_write.indices.create(es_write_index, body=MAPPING_GERRIT)

    query = {"query": {"match_all": {}}}

    items = []
    cont = 1
    uniq_id = 1
    first = True

    for item in helpers.scan(es_read, query, scroll='300m', index=es_read_index):
        items.append(item["_source"])

        if cont % 15000 == 0:
            # Eventizing the first 7500 changesets
            gerrit_events = Gerrit(items)
            events_df = gerrit_events.eventize(2)

            print(cont)
            print(len(events_df))
            # Adding projects information
            events_df = pandas.merge(events_df, projects, how='left', on='repository')

            # Adding gender info
            enriched_gender.data = events_df

            events_df = enriched_gender.enrich("owner")
            events_df = events_df.fillna("Unknown")

            print(len(events_df))
            print(events_df.keys())
            # Add author uuid
            uuids.data = events_df
            events_df["user"] = events_df["owner"]
            events_df = uuids.enrich(['user', 'email'])

            print(len(events_df))
            print(events_df.keys())
            # Uploading info to the new ES
            uniq_id = upload_data(events_df, es_write_index, es_write, uniq_id)

            items = []

        cont = cont + 1

    # helpers.bulk(es_write, docs)
    uniq_id = upload_data(events_df, es_write_index, es_write, uniq_id)


def upload_data(events_df, es_write_index, es_write, uniq_id):
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
    print(len(docs))
    helpers.bulk(es_write, docs)
    items = []

    return uniq_id


def analyze_git(es_read, es_write, es_read_index, es_write_index, key):
    # Retrieve projects information
    projects = openstack_projects()
    projects["repository"] = projects["urls"]
    projects = projects.drop("urls", 1)

    # Retrieve uuids info
    uuids = Uuid(pandas.DataFrame(), file_path='openstack_uuids.csv')

    # Retrieve gender info
    enriched_gender = Gender(pandas.DataFrame(), key, "git_gender.csv")

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

        if cont % 15000 == 0:
            git_events = Git(commits)
            events_df = git_events.eventize(2)

            print(cont)
            print(len(events_df))
            # Filter information
            data_filtered = FilterRows(events_df)
            events_df = data_filtered.filter_(["filepath"], "-")

            # Add filetype info
            enriched_filetype = FileType(events_df)
            events_df = enriched_filetype.enrich('filepath')

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

            # Add author uuid
            uuids.data = events_df
            events_df = uuids.enrich(['user', 'email'])

            print(len(events_df))
            # Add projects information
            events_df = pandas.merge(events_df, projects, how='left', on='repository')
            # Fill NaN projects
            events_df.fillna('notavailable', inplace=True)

            # Deal with surrogates
            convert = ToUTF8(events_df)
            events_df = convert.enrich(
                ["gender_analyzed_name", "committer_gender_analyzed_name", "owner", "committer", "user", "username"])

            print(len(events_df))
            commits = []

            uniq_id = upload_data(events_df, es_write_index, es_write, uniq_id)

        cont = cont + 1
    upload_data(events_df, es_write_index, es_write, uniq_id)


def main():
    es_read, es_write, es_read_git_index, es_read_gerrit_index, \
        es_write_git_index, es_write_gerrit_index, key = ESConnection()

    analyze_gerrit(es_read, es_write, es_read_gerrit_index, es_write_gerrit_index, key)

    analyze_git(es_read, es_write, es_read_git_index, es_write_git_index, key)


if __name__ == "__main__":
    main()
