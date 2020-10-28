FROM fishtownanalytics/dbt:0.18.0

RUN apt-get update && apt-get install -y apt-transport-https ca-certificates gnupg curl
RUN echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN apt-get update && apt-get install -y google-cloud-sdk

ENTRYPOINT ["/bin/bash"]
CMD ["/usr/app/dbt/start.sh"]