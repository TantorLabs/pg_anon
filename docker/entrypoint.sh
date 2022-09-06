#!/bin/bash
set -e

sed -i  '/listen_addresses/s/^#//g' /etc/postgresql/${PG_VERSION}/main/postgresql.conf
sed -ie "s/^listen_addresses.*/listen_addresses = '127.0.0.1'/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
sed -i -e '/local.*peer/s/postgres/all/' -e 's/peer\|md5/trust/g' /etc/postgresql/${PG_VERSION}/main/pg_hba.conf

pg_ctlcluster ${PG_VERSION} main restart

psql -c "ALTER USER postgres WITH PASSWORD 'YmTLbLTLxF'" -U postgres -h 127.0.0.1
psql -c "CREATE USER anon_test_user WITH PASSWORD 'mYy5RexGsZ' SUPERUSER" -U postgres -h 127.0.0.1

ln -s /usr/share/pg_anon/pg_anon.py /usr/bin/pg_anon.py

cat > /usr/bin/pg_anon << EOL
#!/bin/bash
python3 /usr/share/pg_anon/pg_anon.py \$@
EOL

chmod +x /usr/bin/pg_anon

cd /usr/share/pg_anon

echo '[ ! -z "$TERM" -a -r /etc/motd ] && cat /etc/motd' >> /etc/bash.bashrc

trap : TERM INT; sleep infinity & wait