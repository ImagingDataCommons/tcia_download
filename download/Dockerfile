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

