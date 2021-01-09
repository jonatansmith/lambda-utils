import base64
import json
import re
from json import JSONDecoder, JSONDecodeError

print('Loading function')

NOT_WHITESPACE = re.compile(r'[^\s]')

def decode_stacked(document, pos=0, decoder=JSONDecoder()):
    while True:
        match = NOT_WHITESPACE.search(document, pos)
        if not match:
            return
        pos = match.start()

        try:
            obj, pos = decoder.raw_decode(document, pos)
        except JSONDecodeError:
            # do something sensible if there s some error
            raise
        yield obj


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        print(record['recordId'])
        payload = base64.b64decode(record['data']).decode('utf-8')
        payload_with_eachJSON_in_new_line = ''
        for obj in decode_stacked(payload):
            payload_with_eachJSON_in_new_line = payload_with_eachJSON_in_new_line + '\n' + json.dumps(obj) 
        # Do custom processing on the payload here

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(payload_with_eachJSON_in_new_line.encode('utf-8'))
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}

