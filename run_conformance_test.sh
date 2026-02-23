#!/usr/bin/env bash
#
# run the MQTT 3.1.1 + 5.0 conformance tests for a given broker.
#
# the broker is expected to allow anonymous access on unencrypted port 1883
#
function usage {
    printf "Usage:\n"
    printf "$0 --hostname|-h <broker_hostname>\n"
    exit 1
}

function argparse {
  echo "Parameters: $*"

  if [ $# -eq 0 ]; then
      usage
  fi

  while [ $# -gt 0 ]; do
    case "$1" in
      --hostname|-h)
        # the hostname of the MQTT broker
        export BROKER_HOSTNAME="${2}"
        shift
        ;;
      *)
        printf "ERROR: Parameters invalid\n"
        usage
    esac
    shift
  done
}

#
# init
export BROKER_HOSTNAME=localhost
export REPORT_FILE_MQTT_3=conformance-report-mqtt311.txt
export REPORT_FILE_MQTT_5=conformance-report-mqtt5.txt

argparse $*

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

CONFORMANCE_TEST_BINARY=${SCRIPT_DIR}/target/release/mqtt-conformance

if [ -z "${CONFORMANCE_TEST_BINARY}" ]; then
  printf "Test binary not available: '${CONFORMANCE_TEST_BINARY}', please compile it first.\n"
fi

#
# create reports

printf "💥 Creating MQTT 3.1.1 report..."
MQTT_VERSION=3
${CONFORMANCE_TEST_BINARY} run \
  -H ${BROKER_HOSTNAME} -p 1883 \
  --username admin --password admin \
  -v ${MQTT_VERSION} -V | tee ${SCRIPT_DIR}/${REPORT_FILE_MQTT_3}

printf "💥 Creating MQTT 5.0 report..."
MQTT_VERSION=5
${CONFORMANCE_TEST_BINARY} run \
  -H ${BROKER_HOSTNAME} -p 1883 \
  --username admin --password admin \
  -v ${MQTT_VERSION} -V | tee ${SCRIPT_DIR}/${REPORT_FILE_MQTT_5}

printf "✅ MQTT conformance reports created."
