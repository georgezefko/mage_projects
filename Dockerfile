FROM mageai/mageai:0.9.76


ARG PROJECT_NAME=mage
ARG MAGE_CODE_PATH=/home/mage_code
ARG USER_CODE_PATH=${MAGE_CODE_PATH}/${PROJECT_NAME}

WORKDIR ${MAGE_CODE_PATH}


COPY ${PROJECT_NAME} ${PROJECT_NAME}

ENV USER_CODE_PATH=${USER_CODE_PATH}

#-------Spark Related part-------------
# Add Debian Bullseye repository
# RUN echo 'deb http://deb.debian.org/debian bullseye main' > /etc/apt/sources.list.d/bullseye.list

# # Install OpenJDK 11 and wget
# RUN apt-get update -y && \
#     apt-get install -y openjdk-11-jdk wget

# # Remove Debian Bullseye repository
# RUN rm /etc/apt/sources.list.d/bullseye.list
#------------------------------------

# Install custom Python libraries
RUN pip3 install -r ${USER_CODE_PATH}/requirements.txt

RUN python3 /app/install_other_dependencies.py --path ${USER_CODE_PATH}

ENV PYTHONPATH="${PYTHONPATH}:/home/src"

CMD ["/bin/sh", "-c", "/app/run_app.sh"]
