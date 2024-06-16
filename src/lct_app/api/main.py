from fastapi import FastAPI

from common.api.lifespan import lifespan
from common.api.middleware import configure_cors
from lct_app.api.router import lcthack_router

app = FastAPI(
    debug=True,
    title='Lct Hack',
    lifespan=lifespan,
)

configure_cors(app)

# @app.get("/", include_in_schema=False)
# async def redirect_from_root() -> RedirectResponse:
#     return RedirectResponse(url='/docs')


app.include_router(lcthack_router)
