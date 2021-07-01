from fastapi import FastAPI, Header, Response, status
from fastapi.middleware.cors import CORSMiddleware
import boto3
from boto3.dynamodb.conditions import Key, Attr
from typing import Optional
from pydantic import BaseModel
import datetime
import urllib
from urllib import request
import json

#----------------------------------------------------------------------------------------------------------


# Get the service resource - dynamodb:
dynamodb = boto3.resource('dynamodb')

app = FastAPI()


#----------------------------------------------------------------------------------------------------------


origins = [
    "http://localhost:58234",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:62384",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


#----------------------------------------------------------------------------------------------------------


class Dreamer(BaseModel):
    pk: Optional [str]
    sk: Optional[str] 
    name: str
    type: Optional[str]
    dreamer_id: Optional[int]
    dreams: Optional[int]

    
class Dream(BaseModel):
    pk: Optional[str]
    sk: Optional[str]
    content: str
    type: Optional[str]
    dream_id: Optional[int]


    
class Review(BaseModel):
    reviewId: int
    name: str
    object: str
    reason: str
    created: str
    value: int

#----------------------------------------------------------------------------------------------------------

def generate_dreamer_pk_from_given_name(given_name):
    given_name = given_name.upper()
    given_name = given_name.split()
    query_string = ""
    for i in given_name:
        query_string += "#" + i
    return "DREAMER"+query_string
    
def generate_dream_pk_from_given_name(given_name):
    given_name = given_name.upper()
    given_name = given_name.split()
    query_string = ""
    for i in given_name:
        query_string += "#" + i
    return "DREAM"+query_string
    
    
#----------------------------------------------------------------------------------------------------------


@app.get("/", status_code=200)
def read_root():
    return {"message": "Hello from Stadler Peter"}


@app.get("/spring", status_code=200)
def read_root():
    #Get Data from Spring-Boot-Endpoint
    resp = request.urlopen("http://localhost:8080/")
    read_resp =  resp.read()
    decoded_resp = json.loads(read_resp)
    #decoded_resp = read_resp.decode("UTF-8")
    return decoded_resp



#----------------------------------------------------------------------------------------------------------

@app.get("/dreams/number", status_code=200)
def get_dreams_number(httpResponse: Response):
    table = dynamodb.Table('dream_diary')
    response = table.query(
    KeyConditionExpression=Key('pk').eq('total')
    )
    items = response['Items']
    if (items == []):
        httpResponse.status_code = 400
        return {"message": "Fehler! Es gibt keine Numbers. - GET /dreams/number"}
    return {"dreams":items[0]["dreams"]}
    

@app.get("/dreamers/number", status_code=200)
def get_dreams_number(httpResponse: Response):
    table = dynamodb.Table('dream_diary')
    response = table.query(
    KeyConditionExpression=Key('pk').eq('total')
    )
    items = response['Items']
    if (items == []):
        httpResponse.status_code = 400
        return {"message": "Fehler! Es gibt keine Numbers. - GET /dreamers/number"}
    return {"dreamers":items[0]["dreamers"]}

#----------------------------------------------------------------------------------------------------------



    
@app.get("/dreamers", status_code=200)
def get_dreamers(httpResponse: Response):
    table = dynamodb.Table('dream_diary')
    response = table.scan(
    FilterExpression=Attr('type').eq("dreamer")
    )
    items = response['Items']
    listcomp = [i["name"] for i in items]
    if (items == []):
        httpResponse.status_code = 400
        return {"message": "Fehler! Es gibt keine dreamers.  - GET /dreamers"}
    return items

@app.get("/dreamers/{dreamer_name}", status_code=200)
def get_single_dreamer(dreamer_name: str, httpResponse: Response):
    dreamer_name = dreamer_name.upper()
    dreamer_name = dreamer_name.split()
    query_string = ""
    for i in dreamer_name:
        query_string += "#" + i
    table = dynamodb.Table('dream_diary')
    response = table.query(
    KeyConditionExpression=Key('pk').eq('DREAMER' + query_string)
    )
    items = response['Items']
    if (items == []):
        httpResponse.status_code = 400
        return {"message": "Fehler! Dreamer existiert nicht.  - GET /dreamers/{dreamer_name}"}
    return items

@app.post("/dreamers", status_code=200)
def post_single_dreamer(dreamer: Dreamer, httpResponse: Response):
    dreamer.pk = generate_dreamer_pk_from_given_name(dreamer.name)
    table = dynamodb.Table('dream_diary')
    response = table.query(
    KeyConditionExpression=Key('pk').eq(dreamer.pk)
    )
    items = response['Items']
    if (items != []):
        httpResponse.status_code = 400
        return {"message":"Fehler! Dreamer ist already in DynamoDB or ERROR - POST /dreamers", "Dreamer":items}
    dreamer.sk = datetime.datetime.now().strftime("%d.%m.%Y#%H:%M:%S")
    dreamer.type = "dreamer"
    dreamer.dreams = 0
    
    response = table.query(
    KeyConditionExpression=Key('pk').eq('total')
    )
    items = response['Items']
    dreamer.dreamer_id = items[0]["dreamers"]+1
    
    table.update_item(
    Key={
        'pk': 'total',
        'sk': 'total'
    },
    UpdateExpression='SET dreamers = :val1',
    ExpressionAttributeValues={
        ':val1': dreamer.dreamer_id
    }
    )
    response = table.put_item(
    Item=dreamer.__dict__             
    )
    return {"message": "Added dreamer: " + dreamer.name + "  - POST /dreamers", "Dreamer":dreamer} 



#----------------------------------------------------------------------------------------------------------



@app.get("/dreams", status_code=200)
def get_dreams(httpResponse: Response):
    table = dynamodb.Table('dream_diary')
    response = table.scan(
    FilterExpression=Attr('type').eq("dream")
    )
    items = response['Items']
    listcomp = [i["content"]  for i in items]
    if (items == []):
        httpResponse.status_code = 400
        return {"message": "Fehler! Es gibt keine Träume. - GET /dreams"}
    return items

@app.get("/dreams/{dreamer_name}", status_code=200)
def get_dreams_of_one_user(dreamer_name: str, httpResponse: Response):
    dreamer_name = dreamer_name.upper()
    dreamer_name = dreamer_name.split()
    query_string = ""
    for i in dreamer_name:
        query_string += "#" + i
    table = dynamodb.Table('dream_diary')
    
    response = table.query(
    KeyConditionExpression=Key('pk').eq('DREAMER' + query_string)
    )
    items = response['Items']
    if (items == []):
        httpResponse.status_code = 400
        return {"message": "Fehler! Dreamer exisitert nicht. - GET /dreams/{dreamer_name}"}
    
    response = table.query(
    KeyConditionExpression=Key('pk').eq('DREAM' + query_string)
    )
    items = response['Items']
    if (items == []):
        httpResponse.status_code = 400
        return {"message": "Fehler! Dreamer hat keine Träume. - GET /dreams/{dreamer_name}"}
    return items


@app.post("/dreams/{dreamer_name}", status_code=200)
def post_single_dream(dreamer_name: str, dream: Dream, httpResponse: Response):
    dream.pk = generate_dream_pk_from_given_name(dreamer_name)
    dreamer_pk = generate_dreamer_pk_from_given_name(dreamer_name)
    table = dynamodb.Table('dream_diary')
    response = table.query(
    KeyConditionExpression=Key('pk').eq(dreamer_pk)
    )
    items = response['Items']
    dreamer_before = items
    if (items == []):
        httpResponse.status_code = 400
        return {"message":"Fehler! Dreamer exisitert nicht. - POST /dreams/{dreamer_name}"}
    dreamer_new_dreams = dreamer_before[0]["dreams"] + 1
    dreamer_sk = dreamer_before[0]["sk"]
    dream.sk = datetime.datetime.now().strftime("%d.%m.%Y#%H:%M:%S")
    dream.type = "dream"
    
    response = table.query(
    KeyConditionExpression=Key('pk').eq('total')
    )
    items = response['Items']
    dream.dream_id = items[0]["dreams"]+1
   
    table.update_item(
    Key={
        'pk': 'total',
        'sk': 'total'
    },
    UpdateExpression='SET dreams = :val1',
    ExpressionAttributeValues={
        ':val1': dream.dream_id
    }
    )
    
    table.update_item(
    Key={
        'pk': dreamer_pk,
        'sk': dreamer_sk
    },
    UpdateExpression='SET dreams = :val1',
    ExpressionAttributeValues={
        ':val1': dreamer_new_dreams
    }
    )
    
    response = table.put_item(
    Item=dream.__dict__             
    )  
    return {"message": "Added dream for: " + dream.pk + " - POST /dreams/{dreamer_name}", "Dream":dream} 

 
#----------------------------------------------------------------------------------------------------------


@app.get("/reviews")
def read_reviews():
    table = dynamodb.Table('reviews')
    response = table.scan()
    items = response['Items']
    return items
    
@app.get("/cookies")
def read_cookies():
    table = dynamodb.Table('cookies_immobilienscout24')
    response = table.scan()
    items = response['Items']
    return items


@app.get("/items")
def read_items():
    return {"message":"die item_id fehlt"}

@app.get("/item")
def read_item():
    return {"message":"itemS fehlt"}

@app.get("/items/{item_id}")
def read_item_id(item_id: int, q: str):
    if q is not None:
        return {"item_id": item_id, "q": q}
    else:
        return {"item_id": item_id}
    
@app.get("/{variable}")
def wildcard(variable: str):
    return {"message":"This is wildcard-endpoint. Endpunkte: dreamers, dreams"}




#----------------------------------------------------------------------------------------------------------



"""
response = table.get_item(
    Key={
        'dreamerId': 1
    }
)
item = response['Item']
print(item)
"""



"""
@app.get("/dreamer/names")
def read_dreamer_names():
    table = dynamodb.Table('dreamer')
    response = table.scan()
    items = response['Items']
    return items
"""




