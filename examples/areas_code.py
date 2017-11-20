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

from enrich import FileType

from filter import FilterRows


def ESConnection():

    parser = configparser.ConfigParser()
    conf_file = '.settings'
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
            if option == 'index_git_raw': es_read_git_index = parser.get(section, option)

            if option == 'host_output': host_output = parser.get(section, option)
            if option == 'port_output': port_output = parser.get(section, option)
            if option == 'user_output': user_output = parser.get(section, option)
            if option == 'password_output': password_output = parser.get(section, option)
            if option == 'path_output': path_output = parser.get(section, option)
            if option == 'index_git_output': es_write_git_index = parser.get(section, option)


    connection = "https://" + user + ":" + password + "@" + host + ":" + port + "/" + path
    print (connection)
    es_read = Elasticsearch([connection], use_ssl=True, verity_certs=True, ca_cert=certifi.where(), scroll='300m', timeout=100)

    credentials = ""
    if user_output:
        credentials = user_output + ":" + password_output + "@"

    connection_output = "http://" + credentials + host_output + ":" + port_output + "/" + path_output
    #es_write = Elasticsearch([connection_output], use_ssl=True, verity_certs=True, ca_cert=certifi.where(), scroll='300m', timeout=100)
    es_write = Elasticsearch([connection_output])
    print (connection_output)

    return es_read, es_write, es_read_git_index, es_write_git_index


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
    print (len(docs))
    helpers.bulk(es_write, docs)
    items = []

    return uniq_id


def analyze_git(es_read, es_write, es_read_index, es_write_index):

    es_write.indices.delete(es_write_index, ignore=[400, 404])
    #es_write.indices.create(es_write_index, body=MAPPING_GIT)
    es_write.indices.create(es_write_index)

    print ("")
    s = Search(using=es_read, index=es_read_index)
    s.execute()

    commits = []
    cont = 1
    uniq_id = 1
    first = True

    for item in s.scan():
        commits.append(item.to_dict())

        if cont % 100 == 0:
            git_events = events.Git(commits)
            events_df = git_events.eventize(2)

            # Filter information
            data_filtered = FilterRows(events_df)
            events_df = data_filtered.filter_(["filepath"], "-")

            # Add filetype info
            enriched_filetype = FileType(events_df)
            events_df = enriched_filetype.enrich('filepath')

            print(events_df)

            # TODO: Add SortingHat Info

            print (len(events_df))

            # Deal with surrogates
            convert = ToUTF8(events_df)
            events_df = convert.enrich(["owner"])

            print (len(events_df))
            commits = []

            uniq_id = upload_data(events_df, es_write_index, es_write, uniq_id)

        cont = cont + 1
    upload_data(events_df, es_write_index, es_write, uniq_id)

def main():

    es_read, es_write, es_read_git_index, es_write_git_index = ESConnection()

    analyze_git(es_read, es_write, es_read_git_index, es_write_git_index)


if __name__ == "__main__":
    main()
