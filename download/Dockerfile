#
# Copyright 2020, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM google/cloud-sdk
MAINTAINER bcli4d
RUN apt-get -q -y update
RUN apt-get -q -y upgrade
RUN apt-get  install -y -q libcurl4-openssl-dev libssl-dev
RUN rm /usr/bin/python
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ln -s /usr/bin/pip3 /usr/bin/pip
RUN python3 -m pip -q install pycurl
RUN python3 -m pip -q install pydicom
RUN python3 -m pip -q install --upgrade google-cloud-storage
RUN python3 -m pip -q install --upgrade google-cloud-bigquery
RUN mkdir /root/clone_tcia
#RUN mkdir /root/clone_tcia/helpers
#COPY clone_collection.py /root/clone_tcia
COPY application_default_config.json /root/clone_tcia
#COPY helpers/* /root/clone_tcia/helpers/
ENV GOOGLE_APPLICATION_CREDENTIALS=/root/clone_tcia/application_default_config.json
CMD ["/bin/bash"]

