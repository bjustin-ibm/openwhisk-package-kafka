import base64
import json
import ssl

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def main(params):
    validationResult = validateParams(params)
    if validationResult[0] != True:
        return {'error': validationResult[1]}
    else:
        validatedParams = validationResult[1]

    sasl_mechanism = 'PLAIN'
    security_protocol = 'SASL_SSL'

    # Create a new context using system defaults, disable all but TLS1.2
    context = ssl.create_default_context()
    context.options &= ssl.OP_NO_TLSv1
    context.options &= ssl.OP_NO_TLSv1_1

    try:
        producer = KafkaProducer(
            api_version_auto_timeout_ms=15000,
            bootstrap_servers=validatedParams['kafka_brokers_sasl'],
            sasl_plain_username=validatedParams['user'],
            sasl_plain_password=validatedParams['password'],
            security_protocol=security_protocol,
            ssl_context=context,
            sasl_mechanism=sasl_mechanism)

        print("Created producer")

        # only use the key parameter if it is present
        if 'key' in validatedParams:
            messageKey = validatedParams['key']
            producer.send(validatedParams['topic'], str(validatedParams['value']).encode('utf-8'), key=str(messageKey).encode('utf-8'))
        else:
            producer.send(validatedParams['topic'], str(validatedParams['value']).encode('utf-8'))

        producer.flush()

        print("Sent message")
    except NoBrokersAvailable:
        # this exception's message is a little too generic
        return {'error': 'No brokers available. Check that your supplied brokers are correct and available.'}
    except Exception as e:
        return {'error': '{}'.format(e)}

    return {"success": True}

def validateParams(params):
    validatedParams = params.copy()
    requiredParams = ['kafka_brokers_sasl', 'user', 'password', 'topic', 'value']
    missingParams = []

    for requiredParam in requiredParams:
        if requiredParam not in params:
            missingParams.append(requiredParam)

    if len(missingParams) > 0:
        return (False, "You must supply all of the following parameters: {}".format(', '.join(missingParams)))

    # When parameter values are valid JSON strings, they come in as type dict.
    # If this is the case, convert the parameter value into a JSON string.
    if type(params['value']) is dict:
        validatedParams['value'] = json.dumps(params['value'])
        print("Automatically converting value parameter to JSON string: {}".format(validatedParams['value']))

    if 'key' in params and type(params['key']) is dict:
        validatedParams['key'] = json.dumps(params['key'])
        print("Automatically converting key parameter to JSON string: {}".format(validatedParams['key']))

    if 'base64DecodeValue' in params and params['base64DecodeValue'] == True:
        try:
            validatedParams['value'] = base64.b64decode(validatedParams['value']).decode('utf-8')
        except:
            return (False, "value parameter is not Base64 encoded")

        if len(validatedParams['value']) == 0:
            return (False, "value parameter is not Base64 encoded")

    if 'base64DecodeKey' in params and params['base64DecodeKey'] == True:
        try:
            validatedParams['key'] = base64.b64decode(validatedParams['key']).decode('utf-8')
        except:
            return (False, "key parameter is not Base64 encoded")

        if len(validatedParams['key']) == 0:
            return (False, "key parameter is not Base64 encoded")

    return (True, validatedParams)
