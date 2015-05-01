FROM       docker-dev.ops.tune.com/itops/base_centos6:latest
MAINTAINER Yoon Soo Pyon yoon@tune.com

# install jdk and other required utility
RUN yum -y install java-1.7.0-openjdk.x86_64
RUN yum -y install tar wget which

# download maven and install
RUN mkdir -p /usr/local/bin/apache-maven
WORKDIR /usr/local/bin/apache-maven
RUN wget http://mirror.cc.columbia.edu/pub/software/apache/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
RUN tar xvzf apache-maven-3.2.5-bin.tar.gz
WORKDIR /usr/local/bin/apache-maven
RUN ln -s  apache-maven-3.2.5 apache-maven

# maven path set up
ENV M2_HOME=/usr/local/bin/apache-maven/apache-maven
ENV PATH=${M2_HOME}/bin:${PATH}

# test whether maven is correctly installed
RUN mvn --version

# copy git-pulled from outside of docker Shovler code base into docker container 
RUN mkdir -p /usr/local/bin/shoveler
ADD MATDF /usr/local/bin/shoveler/

# maven build for runnable jar 
WORKDIR /usr/local/bin/shoveler
RUN mvn package

# run shoveler
WORKDIR /usr/local/bin/shoveler/target

# manually add aws credential file. TODO:// secure way needed
ADD aws.properties /usr/local/bin/shoveler/
CMD ["java", "-jar", "MATDF-shoveler-0.0.1.jar", "-c", "9", "-p", "2","-a","../aws.properties"]