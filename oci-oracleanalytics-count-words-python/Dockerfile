#https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsfdksandlinuxdistros_faqs.htm

# --Runtime base image
#FROM fnproject/python:3.8 as build-stage

# -- Build-time base image
FROM fnproject/python:3.8-dev as build-stage

WORKDIR /function
ADD requirements.txt /function/
RUN pip3 install --target /python/  --no-cache --no-cache-dir -r requirements.txt &&\
	rm -fr ~/.cache/pip /tmp* requirements.txt func.yaml Dockerfile .venv
ADD . /function/
RUN rm -fr /function/.pip_cache

FROM fnproject/python:3.8
RUN microdnf install oracle-epel-release-el8 &&\
    microdnf install ImageMagick &&\
    microdnf remove oracle-epel-release-el8 &&\
    microdnf install unzip &&\
    microdnf clean all

COPY --from=build-stage /python /python
COPY --from=build-stage /function /function
ENV PYTHONPATH=/function:/python
ENTRYPOINT ["/python/bin/fdk", "/function/func.py", "handler"]