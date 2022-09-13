FROM python:3.7

ENV PROJECT_DIR=datawaves-etl-polygon
RUN mkdir /$PROJECT_DIR
WORKDIR /$PROJECT_DIR
COPY . .
RUN pip install --upgrade pip && pip install git+https://github.com/datawaves-xyz/polygon-etl.git@datawaves#subdirectory=cli

# Add Tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--", "python", "main"]

