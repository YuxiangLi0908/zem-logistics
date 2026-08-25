from fastapi import APIRouter


router = APIRouter(tags=["navigation"])


@router.get("/navigation")
def get_navigation():
    return {
        "status": "transitional",
        "source": "frontend/src/data/navigation.js",
        "message": "Navigation is currently owned by Vue; expose permissions here later.",
    }
