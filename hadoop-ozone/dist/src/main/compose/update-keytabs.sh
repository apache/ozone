set -x

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )

rm -rf "$SCRIPT_DIR/keytabs"
docker run -it --entrypoint=/data/export-keytabs.sh -v "$SCRIPT_DIR":/data -v "$SCRIPT_DIR/keytabs":/etc/security/keytabs elek/ozone-devkrb5:latest

sudo chown -R "$(id -u)" "$SCRIPT_DIR/keytabs"
chmor 755 "$SCRIPT_DIR/keytabs/*.keytab"

SECURE_ENVS=( ozonesecure ozonesecure-mr ozonesecure-om-ha )

for e in "${SECURE_ENVS[@]}"
do
   rm -rf "$SCRIPT_DIR/$e/keytabs"
   cp -r "$SCRIPT_DIR/keytabs" "$SCRIPT_DIR/$e/keytabs"
done


