FROM centos:7.2.1511

ENV container docker

RUN yum -y install iproute httpd

# SystemD, or cgroups, is necessary for mesos-slave, mesos-master does not need it.
RUN yum -y install systemd; yum clean all

# Retz dependencies
RUN yum install -y hwloc hwloc-lib numactl numactl-libs
RUN yum install -y java-1.8.0-openjdk

RUN yum -y install epel-release

RUN yum -y install http://repos.mesosphere.com/el/7/noarch/RPMS/mesosphere-el-repo-7-2.noarch.rpm
RUN yum -y install mesos mesosphere-el-repo mesosphere-zookeeper marathon

# cf. Running systemd within a Docker Container – Red Hat Developer Blog
#   http://developers.redhat.com/blog/2014/05/05/running-systemd-within-docker-container/
RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == systemd-tmpfiles-setup.service ] || rm -f $i; done); \
rm -f /lib/systemd/system/multi-user.target.wants/*;\
rm -f /etc/systemd/system/*.wants/*;\
rm -f /lib/systemd/system/local-fs.target.wants/*; \
rm -f /lib/systemd/system/sockets.target.wants/*udev*; \
rm -f /lib/systemd/system/sockets.target.wants/*initctl*; \
rm -f /lib/systemd/system/basic.target.wants/*;\
rm -f /lib/systemd/system/anaconda.target.wants/*;

# mesos-master can be launced by systemd, not necessarily but simple.
RUN systemctl enable httpd.service
RUN systemctl enable zookeeper.service mesos-master.service
# RUN systemctl enable mesos-slave.service marathon.service

# On the other hand, mesos-master and retz-server need configuration
# with actual IP address. This may be done in service files managed by
# SystemD, but it's dirty workaround to launch them after docker run.
COPY spawn_retz_server.sh /spawn_retz_server.sh
COPY spawn_mesos_slave.sh /spawn_mesos_slave.sh
COPY spawn_retz_server2.sh /spawn_retz_server2.sh
COPY spawn_mesos_agent2.sh /spawn_mesos_agent2.sh

## These files are not used now, better to use?
# COPY mesos-slave_isolation /etc/mesos-slave/isolation
# COPY cgroups_enable_cfs /etc/mesos-slave/cgroups_enable_cfs
COPY mesos-master_default /etc/default/mesos-master

# Copy retz.properties file here, because it will be rewritten by the
# above spawn script for IP address setting.
COPY retz.properties /retz.properties

# Want to know Mesos version
RUN ls -l /usr/lib/libmesos*

CMD ["/usr/sbin/init"]
