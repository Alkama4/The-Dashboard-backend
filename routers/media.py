# External imports
import os
from PIL import Image
from fastapi import HTTPException, Query, APIRouter
from fastapi.responses import FileResponse

# Internal imports
# from utils import query_mysql

# Create the router object for this module
router = APIRouter()


# Could seperate into seperate methods for /title and /service_images, but this just does it automatically
@router.get("/image/{image_path:path}")
async def get_image(image_path: str, width: int = Query(None)):
    base_path = "/fastapi-media"
    full_path = os.path.join(base_path, image_path)

    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="Image doesn't exist.")
    
    if width and width in [300, 600, 900, 1200]:
        resized_path = f"{os.path.splitext(full_path)[0]}_{width}{os.path.splitext(full_path)[1]}"
        if not os.path.exists(resized_path):
            img = Image.open(full_path)
            aspect_ratio = img.height / img.width
            new_height = int(width * aspect_ratio)
            img = img.resize((width, new_height))
            img.save(resized_path)

        return FileResponse(resized_path)

    return FileResponse(full_path)