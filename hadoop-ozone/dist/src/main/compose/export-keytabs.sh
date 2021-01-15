#!/bin/sh

export_keytab() {
   kadmin.local -q "ktadd -norandkey -k /etc/security/keytabs/$2.keytab $1@EXAMPLE.COM"
}

rm /etc/security/keytabs/*.keytab

export_keytab scm/scm scm
export_keytab HTTP/scm scm
export_keytab testuser/scm scm
export_keytab testuser2/scm scm

export_keytab om/om om
export_keytab HTTP/om om

export_keytab s3g/s3g s3g
export_keytab HTTP/s3g s3g
export_keytab testuser/s3g s3g

export_keytab recon/recon recon
export_keytab HTTP/recon recon

export_keytab dn/dn dn
export_keytab HTTP/dn dn

chmod 755 /etc/security/keytabs/*.keytab
