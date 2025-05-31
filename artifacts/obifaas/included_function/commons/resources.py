import requests
import boto3
import botocore
import io
import torch.nn.functional as F
import torch
from PIL import Image
from torchvision import transforms

class PredictResource:
    def __init__(self):
        client_config = botocore.config.Config(
            max_pool_connections=1000,
        )

        self.session = boto3.session.Session()
        print("Session created")
        self.s3_client = self.session.client('s3', config=client_config)

    def loadimage(self, image):
        with open(image, 'rb') as file:
            file_content = file.read()
            return file_content

    def downloadimage(self, key, s3_bucket):
        if 'http' in key:
            response = requests.get(key)
            object_data = response.content
        else:
            #print(f"Loading {key} from S3")
            url = self.s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket': s3_bucket, 'Key': key},
                                                        ExpiresIn=3600)
            print("Response from S3: ", url)
            response = requests.get(url)
            object_data = response.content
            print("Weight of object_data: ", len(object_data))
            print(f"Type of object_data: {type(object_data)}")
        return object_data

    def transform_image(self, image_data):
        composed_transforms = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std=[0.229, 0.224, 0.225]
            ),
        ])
        print("Opening image")
        image = Image.open(io.BytesIO(image_data)).convert('RGB')
        print("Transforming image")
        transformed_img = composed_transforms(image)
        print("Transformation finished")
        return transformed_img
