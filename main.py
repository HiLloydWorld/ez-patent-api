from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from pypdf import PdfReader, PdfWriter
import httpx
import asyncio
import tempfile
import os
import re
import zipfile

# ==========================================
# 1. 建立應用程式與設定 CORS
# ==========================================
app = FastAPI(title="EZ-Patents Clone API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 2. 資料模型定義 (接收前端傳來的資料)
# ==========================================
class DocumentMeta(BaseModel):
    document_id: str
    description: str
    date: str

class MergeRequest(BaseModel):
    documents: List[DocumentMeta]

# ==========================================
# 3. 共用輔助函式
# ==========================================
def normalize_app_number(app_num: str) -> str:
    """過濾非數字字元，例如 14/472,023 轉成 14472023"""
    cleaned = re.sub(r'\D', '', app_num)
    if not cleaned:
        raise ValueError("無效的申請號格式")
    return cleaned

def sanitize_filename(filename: str) -> str:
    """過濾特殊字元，避免 Windows/Mac 解壓縮路徑錯誤"""
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

async def fetch_pdf_stream(url: str):
    """單檔下載用的串流產生器，每次回傳 64KB"""
    async with httpx.AsyncClient() as client:
        async with client.stream("GET", url, timeout=30.0) as response:
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="找不到該文件，可能尚未公開。")
            elif response.status_code != 200:
                raise HTTPException(status_code=response.status_code, detail="USPTO 伺服器回應異常。")
            
            async for chunk in response.aiter_bytes(chunk_size=65536):
                yield chunk

async def download_pdf_to_disk(client: httpx.AsyncClient, url: str, save_path: str, semaphore: asyncio.Semaphore):
    """批次下載用的檔案寫入器 (受併發連線數限制)"""
    async with semaphore:
        async with client.stream("GET", url, timeout=45.0) as response:
            if response.status_code != 200:
                raise Exception(f"下載失敗: {url}")
            
            with open(save_path, 'wb') as f:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    f.write(chunk)

def cleanup_temp_dir(temp_dir: tempfile.TemporaryDirectory):
    """背景任務：傳輸完畢後刪除暫存檔"""
    temp_dir.cleanup()

# ==========================================
# 4. API 路由端點
# ==========================================

# [功能 1] 取得專利歷史文件清單
@app.get("/api/patent/{app_num}/documents")
async def get_patent_documents(app_num: str):
    try:
        clean_num = normalize_app_number(app_num)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    uspto_url = f"https://ws.globaldossier.uspto.gov/api/doclist/US/US{clean_num}"

    # 加上瀏覽器偽裝標頭，避免被 USPTO 阻擋
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }

    async with httpx.AsyncClient(headers=headers) as client:
        try:
            # ... (底下的程式碼都不變)
            

# [功能 2] 單一文件 PDF 串流下載
@app.get("/api/patent/{app_num}/document/{document_id}/download")
async def download_single_document(app_num: str, document_id: str):
    try:
        clean_num = normalize_app_number(app_num)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    uspto_pdf_url = f"https://ws.globaldossier.uspto.gov/api/doc/US/US{clean_num}/{document_id}/pdf"
    
    headers = {
        "Content-Disposition": f'attachment; filename="US_{clean_num}_{document_id}.pdf"',
        "Cache-Control": "no-cache"
    }

    return StreamingResponse(
        fetch_pdf_stream(uspto_pdf_url),
        media_type="application/pdf",
        headers=headers
    )

# [功能 3] 多份 PDF 合併並加入書籤
@app.post("/api/patent/{app_num}/merge")
async def merge_patent_documents(app_num: str, request: MergeRequest, background_tasks: BackgroundTasks):
    clean_num = normalize_app_number(app_num)
    sorted_docs = sorted(request.documents, key=lambda x: x.date)
    
    temp_dir = tempfile.TemporaryDirectory()
    semaphore = asyncio.Semaphore(5)
    tasks = []
    file_mapping = {}
    
    async with httpx.AsyncClient() as client:
        for doc in sorted_docs:
            url = f"https://ws.globaldossier.uspto.gov/api/doc/US/US{clean_num}/{doc.document_id}/pdf"
            file_path = os.path.join(temp_dir.name, f"{doc.document_id}.pdf")
            file_mapping[doc.document_id] = file_path
            tasks.append(download_pdf_to_disk(client, url, file_path, semaphore))
            
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            temp_dir.cleanup()
            raise HTTPException(status_code=500, detail=f"文件下載失敗: {str(e)}")

    merger = PdfWriter()
    output_filename = f"US_{clean_num}_IFW_Merged.pdf"
    output_filepath = os.path.join(temp_dir.name, output_filename)
    
    try:
        for doc in sorted_docs:
            pdf_path = file_mapping[doc.document_id]
            reader = PdfReader(pdf_path)
            bookmark_title = f"{doc.date} - {doc.description}"
            merger.append(reader, outline_item=bookmark_title)
            
        with open(output_filepath, "wb") as f_out:
            merger.write(f_out)
            
    except Exception as e:
        temp_dir.cleanup()
        raise HTTPException(status_code=500, detail=f"PDF 合併錯誤: {str(e)}")

    background_tasks.add_task(cleanup_temp_dir, temp_dir)
    return FileResponse(path=output_filepath, filename=output_filename, media_type="application/pdf")

# [功能 4] 多份 PDF 打包成單一 ZIP
@app.post("/api/patent/{app_num}/zip")
async def zip_patent_documents(app_num: str, request: MergeRequest, background_tasks: BackgroundTasks):
    clean_num = normalize_app_number(app_num)
    
    temp_dir = tempfile.TemporaryDirectory()
    semaphore = asyncio.Semaphore(5)
    tasks = []
    file_mapping = {}
    
    async with httpx.AsyncClient() as client:
        for doc in request.documents:
            url = f"https://ws.globaldossier.uspto.gov/api/doc/US/US{clean_num}/{doc.document_id}/pdf"
            file_path = os.path.join(temp_dir.name, f"{doc.document_id}.pdf")
            file_mapping[doc.document_id] = file_path
            tasks.append(download_pdf_to_disk(client, url, file_path, semaphore))
            
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            temp_dir.cleanup()
            raise HTTPException(status_code=500, detail=f"文件下載失敗: {str(e)}")

    zip_filename = f"US_{clean_num}_IFW.zip"
    zip_filepath = os.path.join(temp_dir.name, zip_filename)
    
    try:
        with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for doc in request.documents:
                pdf_path = file_mapping[doc.document_id]
                safe_desc = sanitize_filename(doc.description)
                archive_name = f"{doc.date} - {safe_desc} - {doc.document_id}.pdf"
                zipf.write(pdf_path, arcname=archive_name)
                
    except Exception as e:
        temp_dir.cleanup()
        raise HTTPException(status_code=500, detail=f"ZIP 打包錯誤: {str(e)}")

    background_tasks.add_task(cleanup_temp_dir, temp_dir)
    return FileResponse(path=zip_filepath, filename=zip_filename, media_type="application/zip")
