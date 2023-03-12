# Cephsum Server
A server based version of the cephsum package to decouple the ceph based operations from the clients calls.


# dependencies
```
requests
psutil
```


# RPM building 
```
python3 setup.py bdist_rpm
```

# Config file
```
[APP]
loglevel = debug
secretsfile = cephsum-secrets.cfg
host = 0.0.0.0
port = 1781
logfile = log.log

[CEPHSUM]
lfn2pfn = storage.xml
readsize = 64
maxpoolsize = 5
actions = stat,cksum,ping,wait

[CEPH]
cephconf = /etc/ceph/ceph.conf
keyring = /etc/ceph/ceph.client.user.keyring
cephuser = client.user
```

# secrets file
This file (e.g. cephsum-secrets.cfg) should contain only a single string for the shared secret key, and be well protected (e.g. permissions)


