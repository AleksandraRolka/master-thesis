import json
import os
import traceback
from bs4 import BeautifulSoup
from google.cloud import storage
from common_preprocessing_utils import remove_emojis, clean_text, convert_to_unix_time


# Create a client for Google Cloud Storage
client = storage.Client()

# Define the names of the input and output files and buckets
input_bucket_name = "twibot-22-bucket-parsed-english-only"
output_bucket_name = "twibot-22-bucket-preprocessed"
filename_base = "tweets_"
json_extension = ".json"
num_of_files = 106


# Iterate over all diveded data files:
for indx in range(num_of_files):
    input_file = filename_base + str(indx).zfill(3) + json_extension
    output_file = input_file
    print("Input file: ", input_file)

    input_bucket = client.bucket(input_bucket_name)
    input_blob = input_bucket.blob(input_file)
    org_input_data = input_blob.download_as_string().decode("utf-8")

    input_data = []
    for line in org_input_data.splitlines():
        input_data.append(json.loads(line))

    print("Preprocessing file data..")

    for item in input_data:
        with open(output_file, "a+") as f:
            try:
                # Clear new structure's fields 
                transformed = {}
                transformed['id'] = None
                transformed['author_id'] = ""
                transformed['created_at'] = ""
                transformed['org_text'] = ""
                transformed['text'] = ""
                transformed['source'] = None
                transformed['withheld'] = False 
                transformed['copyright_infringement'] = False 
                transformed['is_reply'] = False
                transformed['geo_tagged'] = False
                transformed['latitude'] = None
                transformed['longitude'] = None
                transformed['conversation_id'] = None
                transformed['reply_settings'] = None
                transformed['retweet_count'] = 0
                transformed['reply_count'] = 0
                transformed['like_count'] = 0
                transformed['quote_count'] = 0
                transformed['any_polls_attached'] = False
                transformed['any_media_attached'] = False
                transformed['possibly_sensitive'] = False
                transformed['has_referenced_tweets'] = False
                transformed['media_attached'] = False
                transformed['no_cashtags'] = 0
                transformed['no_mentions'] = 0
                transformed['no_user_mentions'] = 0
                transformed['user_mentions'] = []
                transformed['no_urls'] = 0
                transformed['contains_images'] = False
                transformed['contains_annotations'] = False
                transformed['no_hashtags'] = 0
                transformed['hashtags'] = []
                transformed['context_annotations_domain_id'] = None
                transformed['context_annotations_domain_name'] = None
                transformed['context_annotations_entity_id'] = None
                transformed['context_annotations_entity_name'] = None

                # column source: extract only names
                if ('source' in item and item['source']):
                    source = item['source']
                    soup = BeautifulSoup(item['source'], "html.parser")
                    links = soup.find_all('a')
                    source = links[0].string if len(soup.find_all('a')) > 0 else source
                    transformed['source'] = source
                # column reply_settings: should be preprocessed later: categorical encoding
                if ('reply_settings' in item):
                    transformed['reply_settings'] = item['reply_settings'] 
                # column public_metrics: retweet_count, reply_count, like_count, quote_count : flatten structure, remove top level field
                if ('public_metrics' in item and item['public_metrics']):
                    if ('retweet_count' in item['public_metrics'] and item['public_metrics']['retweet_count']):
                        transformed['retweet_count'] = item['public_metrics']['retweet_count']
                    if ('reply_count' in item['public_metrics'] and item['public_metrics']['reply_count']):
                        transformed['reply_count'] = item['public_metrics']['reply_count']
                    if ('like_count' in item['public_metrics'] and item['public_metrics']['like_count']):
                        transformed['like_count'] = item['public_metrics']['like_count']
                    if ('quote_count' in item['public_metrics'] and item['public_metrics']['quote_count']):
                        transformed['quote_count'] = item['public_metrics']['quote_count']
                # column lang: remove whole column (tweetes already filtered to only english)
                # if (('lang' in item) and item['lang']):
                    # del transformed['lang']
                # column id: -
                if ('id' in item):
                    transformed['id'] = item['id']
                # column in_reply_to_user_id: change column name to "is_reply", map to boolean value 
                if (('in_reply_to_user_id' in item)):
                    transformed['is_reply'] = True if item['in_reply_to_user_id'] else False
                # column geo: { coordinates { type, latitude, longitude }, type, place_id } : transform to separete column: latitude, longitude, geo_type, geo_place_id
                if ('geo' in item and item['geo'] and 'coordinates' in item['geo'] and item['geo']['coordinates']):
                    if ('latitude' in item['geo']['coordinates'] and 'longitude' in item['geo']['coordinates']):
                        transformed['geo_tagged'] = True
                        transformed['latitude'] = item['geo']['coordinates']['latitude']
                        transformed['longitude'] = item['geo']['coordinates']['longitude']
                    # transformed['geo_type'] = item['geo']['type'] if (item['geo']) else None
                    # transformed['geo_place_id'] = item['geo']['place_id'] if (item['geo']) else None
                # column entities: complex structure
                #
                # entities: {
                #     media: {
                # 		source_user_id,
                #         sizes: { 
                # 			large: { resize, h, w }, 
                # 			medium: { resize, h, w } , 
                # 			small: { resize, h, w } , 
                # 			thumb: { resize, h, w } },
                # 		type,
                # 		id_str,
                # 		display_url,
                # 		url,
                # 		expanded_url,
                # 		id,
                # 		indices,
                # 		media_url_https,
                # 		source_user_id_str,
                # 		media_url,
                # 		source_status_id_str,
                # 		source_status_id
                #     }
                #     cashtags: { start, end, tag },
                #     mentions: {start, end, username, id }
                #     user_mentions: { id_str, id, name, indices, screen_name },
                #     symbols: { indices, text }
                #     urls: { indices, display_url, expanded_url, url, unwound_url, title, description, start, end, status },
                #     images: { url, width, height }
                #     annotations: {
                # 		domain: { id, name, description },
                #       type,
                # 		start,
                # 		end,
                # 		probability,
                #         entity: { id, name, description, normalized_text }
                # 	},
                #     hashtags:{ indices, tag, text, start, end }
                # }
                # 
                # leave only few field as a new separate column: 
                #   media_attached - true/false,
                #   media_type - mapped: if tweet contains media: photo, video, animated_gif else None
                #   no_cashtags - number of cashtags, 
                #   no_mentions - number of mentions, 
                #   no_user_mentions - number of user mentions, 
                #   user_mentions: { id_str, username }
                #   no_urls - number of user urls included in tweet, 
                #   contains_images - true/false, 
                #   contains_annotations - true/false, 
                #   no_hashtags - number of hashtags, 
                #   hashtags: tag (name) (from hashtags.text) should be preprocessed later: integer encoding
                #   
                if ('entities' in item and item['entities']):
                    if ('media' in item and item['entities']['media']):
                        transformed['media_attached'] = True
                    if ('cashtags' in item['entities'] and item['entities']['cashtags']):
                        transformed['no_cashtags'] = len(item['entities']['cashtags'])
                    if ('mentions' in item['entities'] and item['entities']['mentions']):
                        transformed['no_mentions'] = len(item['entities']['mentions'])
                    if ('user_mentions' in item['entities'] and item['entities']['user_mentions']):
                        transformed['no_user_mentions'] = len(item['entities']['user_mentions'])
                        for dict in item['entities']['user_mentions']:
                            inner_dict = {'id': dict['id_str'], 'username': dict['screen_name']}
                            transformed['user_mentions'].append(inner_dict)
                    if ('urls' in item['entities'] and item['entities']['urls']):
                        transformed['no_urls'] = len(item['entities']['urls'])
                    if('images' in item['entities'] and len(item['entities']['images']) > 0):
                        transformed['contains_images'] = True 
                    if('annotations' in item['entities'] and len(item['entities']['annotations']) > 0):
                        transformed['contains_annotations'] = True
                    if ('hashtags' in item['entities'] and item['entities']['hashtags']):
                        transformed['no_hashtags'] = len(item['entities']['hashtags'])
                        for dict in item['entities']['hashtags']:
                            if ('text' in dict and dict['text']):
                                inner_dict = {'tagname': dict['text']}
                                transformed['hashtags'].append(inner_dict)
                            elif ('tag' in dict and dict['tag']):
                                inner_dict = {'tagname': dict['tag']}
                                transformed['hashtags'].append(inner_dict)
                    
                # column text:
                #   - removal of punctuation, numbers, special characters, emojis, HTML escape characters, urls, emails
                #   - conversion to lowercase
                if ('text' in item):
                    transformed['org_text'] = item['text']
                    tweet_text = item['text']
                    tweet_text = remove_emojis(tweet_text)
                    tweet_text = clean_text(tweet_text)
                    transformed['text'] = tweet_text
                # column created_at
                if ('created_at' in item):
                    datetime_string = item['created_at']
                    transformed['created_at'] = convert_to_unix_time(datetime_string)
                # column withheld: {copyright, country_codes, scope }
                # simplified info: two separate boolean column: withheld, copyright_infringement
                if ('withheld' in item and item['withheld']):
                    transformed['withheld'] = True 
                    if 'copyright' in item['withheld'] and item['withheld']['copyright'] is True:
                        transformed['copyright_infringement'] = True
                # column attachments: {media_keys, poll_ids }
                # simplified info: two separate boolean column: any_polls_attached, any_media_attached
                if ('attachments' in item and item['attachments']):
                    if ('poll_ids' in item['attachments'] and len(item['attachments']['poll_ids']) > 0):
                        transformed['any_polls_attached'] = True
                    if ('media_keys' in item['attachments'] and len(item['attachments']['media_keys']) > 0):
                        transformed['any_media_attached'] = True
                # column conversation_id: -
                if ('conversation_id' in item):
                    transformed['conversation_id'] = item['conversation_id']
                # column possibly_sensitive: -
                if ('possibly_sensitive' in item):
                    transformed['possibly_sensitive'] = item['possibly_sensitive']
                # column referenced_tweets:
                if ('referenced_tweets' in item and item['referenced_tweets']):
                    transformed['has_referenced_tweets'] = True
                # column context_annotations:
                # description unneccessary, other fields like entities and domain flatten, taken only first elements from list (most of records has only one or null)
                if ('context_annotations' in item and item['context_annotations']):
                    if ('domain' in item['context_annotations']):
                        if (item['context_annotations']['domain'][0]['id']):
                            transformed['context_annotations_domain_id'] = item['context_annotations']['domain'][0]['id']
                        if (item['context_annotations']['domain'][0]['name']):
                            transformed['context_annotations_domain_name'] = item['context_annotations']['domain'][0]['name']
                    if ('entity' in item['context_annotations']):
                        if (item['context_annotations']['entity'][0]['id']):
                            transformed['context_annotations_entity_id'] = item['context_annotations']['entity'][0]['id']
                        if (item['context_annotations']['entity'][0]['name']):
                            transformed['context_annotations_entity_name'] = item['context_annotations']['entity'][0]['name']
                if ('author_id' in item):
                    transformed['author_id'] = item['author_id']

                f.write(json.dumps(transformed) + "\n")

            except Exception as e: 
                print("Tweet before:")
                print(item)
                print("Tweet after:")
                print(transformed)
                print("Error message:")
                print(traceback.print_exc())

    print("Saving data to storage bucket..")

    # Store the output file in Google Cloud Storage
    output_bucket = client.bucket(output_bucket_name)
    output_blob = output_bucket.blob(output_file)
    output_blob.upload_from_filename(output_file)

    path = "./" + output_file
    os.remove(path)