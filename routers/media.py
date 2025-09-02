# External imports
import os
from PIL import Image
from fastapi import HTTPException, Query, APIRouter
from fastapi.responses import FileResponse
from starlette.concurrency import run_in_threadpool

# Internal imports
from utils import ALLOWED_WIDTHS, MEDIA_BASE_PATH

# Create the router object for this module
router = APIRouter()

ALLOWED_WIDTHS = {300, 600, 900, 1200}
BASE_PATH = "/fastapi-media"


def resize_and_save_image(original_path: str, output_path: str, width: int):
    with Image.open(original_path) as img:
        aspect_ratio = img.height / img.width
        new_height = int(width * aspect_ratio)
        resized = img.resize((width, new_height))
        resized.save(output_path)

# Could seperate into seperate methods for /title and /service_images, but this just does it automatically
@router.get("/image/{image_path:path}")
async def get_image(image_path: str, width: int = Query(None)):
    full_path = os.path.join(BASE_PATH, image_path)

    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="Image doesn't exist.")
    
    if width and width in ALLOWED_WIDTHS:
        resized_path = f"{os.path.splitext(full_path)[0]}_{width}{os.path.splitext(full_path)[1]}"
        
        if not os.path.exists(resized_path):
            await run_in_threadpool(resize_and_save_image, full_path, resized_path, width)

        return FileResponse(resized_path)

    return FileResponse(full_path)
