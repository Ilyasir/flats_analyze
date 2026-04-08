from fastapi import APIRouter, Response

router = APIRouter(tags=["SEO"])


@router.get("/robots.txt")
async def get_robots():
    content = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /admin\n"
        "Disallow: /profile\n"
        "Disallow: /auth\n"
        "\n"
        "Sitemap: http://localhost:8000/sitemap.xml"
    )
    return Response(content=content, media_type="text/plain")


@router.get("/sitemap.xml")
async def get_sitemap():
    content = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <url>
            <loc>http://localhost:5173/</loc>
            <lastmod>2026-04-07</lastmod>
            <changefreq>daily</changefreq>
            <priority>1.0</priority>
        </url>
        <url>
            <loc>http://localhost:5173/predict</loc>
            <lastmod>2026-04-07</lastmod>
            <changefreq>monthly</changefreq>
            <priority>0.8</priority>
        </url>
    </urlset>"""
    return Response(content=content, media_type="application/xml")
