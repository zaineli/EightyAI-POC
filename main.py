from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pytesseract
from PIL import Image
import pdf2image
import httpx
import uuid
import json
from datetime import datetime
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="PDF OCR + LLM Processing API", version="1.0.0")

# Configure CORS for your Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Your Next.js app URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
UPLOAD_DIR = Path("uploads")
OCR_RESULTS_DIR = Path("ocr_results")
PROCESSED_RESULTS_DIR = Path("processed_results")
MAX_FILE_SIZE = 10 * 1024 * 1024  
OLLAMA_URL = "http://localhost:11434" 
MODEL_NAME = "llama3.2:3b"

UPLOAD_DIR.mkdir(exist_ok=True)
OCR_RESULTS_DIR.mkdir(exist_ok=True)
PROCESSED_RESULTS_DIR.mkdir(exist_ok=True)

class ProcessRequest(BaseModel):
    system_prompt: str
    user_prompt: str = None

# Default system prompt
DEFAULT_SYSTEM_PROMPT = """
You are an intelligent document processing assistant. Your task is to extract structured information from invoices and organize it into a tabular (columnar) format in an Excel document called 'Testing Workpaper'.

a. Invoice date	
b. Invoice ID	
c. Customer name	
d. Invoice amount (No VAT)	
e. VAT	
f. Total Amount

Ensure the extracted data is clean, accurate, and placed under the correct column headers in the Excel file.
"""

@app.get("/")
async def root():
    return {"message": "PDF OCR + LLM Processing API is running"}

@app.post("/upload-pdf")
async def upload_pdf(
    file: UploadFile = File(...),
    system_prompt: str = DEFAULT_SYSTEM_PROMPT,
    user_prompt: str = "Please analyze this document and provide a comprehensive summary."
):
    """
    Upload a PDF file, process it with OCR, and analyze with Llama
    """
    try:
        # Validate file type
        if file.content_type != "application/pdf":
            raise HTTPException(
                status_code=400, 
                detail="Only PDF files are allowed"
            )
        
        # Read file content
        file_content = await file.read()
        
        # Validate file size
        if len(file_content) > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=400,
                detail=f"File size exceeds {MAX_FILE_SIZE / (1024*1024)}MB limit"
            )
        
        # Generate unique filename
        file_id = str(uuid.uuid4())
        file_extension = Path(file.filename).suffix
        unique_filename = f"{file_id}{file_extension}"
        file_path = UPLOAD_DIR / unique_filename
        
        # Save uploaded file
        with open(file_path, "wb") as buffer:
            buffer.write(file_content)
        
        logger.info(f"File saved: {file_path}")
        
        # Process PDF with OCR
        ocr_results = await process_pdf_with_ocr(file_path, file_id)
        
        # Process with Llama
        llm_response = await process_with_llama(
            ocr_results["full_text"], 
            system_prompt, 
            user_prompt,
            file_id
        )
        
        # Prepare response
        response_data = {
            "message": "PDF uploaded, OCR processed, and analyzed successfully",
            "file_id": file_id,
            "filename": unique_filename,
            "file_path": str(file_path),
            "pages_processed": len(ocr_results["pages"]),
            "ocr_results": ocr_results,
            "llm_analysis": llm_response,
            "upload_timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Complete processing finished for {unique_filename}")
        
        return JSONResponse(content=response_data)
        
    except Exception as e:
        logger.error(f"Error processing PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing PDF: {str(e)}")

async def process_pdf_with_ocr(pdf_path: Path, file_id: str):
    """
    Convert PDF to images and apply OCR using Tesseract
    """
    try:
        # Convert PDF to images
        logger.info(f"Converting PDF to images: {pdf_path}")
        pages = pdf2image.convert_from_path(
            pdf_path,
            dpi=300, 
            fmt='PNG'
        )
        
        ocr_results = {
            "file_id": file_id,
            "total_pages": len(pages),
            "pages": [],
            "full_text": "",
            "processing_timestamp": datetime.now().isoformat()
        }
        
        full_text_parts = []
        
        # Process each page
        for page_num, page_image in enumerate(pages, 1):
            logger.info(f"Processing page {page_num}/{len(pages)}")
            
            page_text = pytesseract.image_to_string(
                page_image,
                config='--oem 3 --psm 6' 
            )
            
            # Get detailed OCR data (with confidence scores and bounding boxes)
            page_data = pytesseract.image_to_data(
                page_image,
                output_type=pytesseract.Output.DICT,
                config='--oem 3 --psm 6'
            )
            
            # Filter out empty text and low confidence results
            filtered_words = []
            for i in range(len(page_data['text'])):
                if int(page_data['conf'][i]) > 30 and page_data['text'][i].strip():  # Confidence > 30%
                    filtered_words.append({
                        'text': page_data['text'][i],
                        'confidence': int(page_data['conf'][i]),
                        'bbox': {
                            'x': page_data['left'][i],
                            'y': page_data['top'][i],
                            'width': page_data['width'][i],
                            'height': page_data['height'][i]
                        }
                    })
            
            page_result = {
                "page_number": page_num,
                "text": page_text.strip(),
                "word_count": len(page_text.split()),
                "words_with_positions": filtered_words,
                "average_confidence": sum(word['confidence'] for word in filtered_words) / len(filtered_words) if filtered_words else 0
            }
            
            ocr_results["pages"].append(page_result)
            full_text_parts.append(page_text.strip())
        
        # Combine all text
        ocr_results["full_text"] = "\n\n".join(full_text_parts)
        ocr_results["total_words"] = len(ocr_results["full_text"].split())
        
        # Save OCR results to file
        results_file = OCR_RESULTS_DIR / f"{file_id}_ocr_results.json"
        with open(results_file, "w", encoding="utf-8") as f:
            json.dump(ocr_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"OCR results saved to: {results_file}")
        
        return ocr_results
        
    except Exception as e:
        logger.error(f"Error in OCR processing: {str(e)}")
        raise Exception(f"OCR processing failed: {str(e)}")

async def process_with_llama(extracted_text: str, system_prompt: str, user_prompt: str, file_id: str):
    """
    Process the extracted text with local Llama model
    """
    try:
        logger.info("Processing with Llama model...")
        
        # Prepare the prompt
        full_prompt = f"{user_prompt}\n\nDocument content:\n{extracted_text}"
        
        # Prepare request payload for Ollama
        payload = {
            "model": MODEL_NAME,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": full_prompt}
            ],
            "stream": False,
            "options": {
                "temperature": 0.6,
                "max_tokens": 2048
            }
        }
        
        # Make request to Ollama
        async with httpx.AsyncClient(timeout=300.0) as client: 
            response = await client.post(
                f"{OLLAMA_URL}/api/chat",
                json=payload
            )
            
            if response.status_code != 200:
                raise Exception(f"Ollama API error: {response.status_code} - {response.text}")
            
            result = response.json()
            
        llm_response = {
            "model_used": MODEL_NAME,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "response": result["message"]["content"],
            "processing_timestamp": datetime.now().isoformat(),
            "total_tokens": result.get("eval_count", 0),
            "prompt_tokens": result.get("prompt_eval_count", 0)
        }
        
        # Save LLM results
        results_file = PROCESSED_RESULTS_DIR / f"{file_id}_llm_results.json"
        with open(results_file, "w", encoding="utf-8") as f:
            json.dump(llm_response, f, indent=2, ensure_ascii=False)
        
        logger.info("Llama processing completed successfully")
        
        return llm_response
        
    except Exception as e:
        logger.error(f"Error in Llama processing: {str(e)}")
        raise Exception(f"Llama processing failed: {str(e)}")

@app.post("/process-text")
async def process_text_with_llama(request: ProcessRequest):
    """
    Process any text with Llama (without OCR)
    """
    try:
        response = await process_with_llama(
            extracted_text="",
            system_prompt=request.system_prompt,
            user_prompt=request.user_prompt or "Please help me with this request.",
            file_id=str(uuid.uuid4())
        )
        
        return JSONResponse(content=response)
        
    except Exception as e:
        logger.error(f"Error processing text with Llama: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing text: {str(e)}")

@app.get("/ocr-results/{file_id}")
async def get_ocr_results(file_id: str):
    """
    Retrieve OCR results for a specific file
    """
    try:
        results_file = OCR_RESULTS_DIR / f"{file_id}_ocr_results.json"
        
        if not results_file.exists():
            raise HTTPException(status_code=404, detail="OCR results not found")
        
        with open(results_file, "r", encoding="utf-8") as f:
            ocr_results = json.load(f)
        
        return JSONResponse(content=ocr_results)
        
    except Exception as e:
        logger.error(f"Error retrieving OCR results: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving OCR results")

@app.get("/llm-results/{file_id}")
async def get_llm_results(file_id: str):
    """
    Retrieve LLM analysis results for a specific file
    """
    try:
        results_file = PROCESSED_RESULTS_DIR / f"{file_id}_llm_results.json"
        
        if not results_file.exists():
            raise HTTPException(status_code=404, detail="LLM results not found")
        
        with open(results_file, "r", encoding="utf-8") as f:
            llm_results = json.load(f)
        
        return JSONResponse(content=llm_results)
        
    except Exception as e:
        logger.error(f"Error retrieving LLM results: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving LLM results")

@app.get("/llm-results/")
async def get_latest_llm_result():
    """
    Retrieve the latest LLM analysis result (no file_id required)
    """
    try:
        # Find the most recently modified LLM result file
        llm_files = list(PROCESSED_RESULTS_DIR.glob("*_llm_results.json"))
        if not llm_files:
            raise HTTPException(status_code=404, detail="No LLM results found")
        latest_file = max(llm_files, key=lambda f: f.stat().st_mtime)
        with open(latest_file, "r", encoding="utf-8") as f:
            llm_results = json.load(f)
        return JSONResponse(content=llm_results)
    except Exception as e:
        logger.error(f"Error retrieving latest LLM result: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving latest LLM result")

@app.get("/health/ollama")
async def check_ollama_health():
    """
    Check if Ollama is running and the model is available
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Check if Ollama is running
            response = await client.get(f"{OLLAMA_URL}/api/tags")
            
            if response.status_code != 200:
                return {"status": "error", "message": "Ollama not accessible"}
            
            models = response.json()
            available_models = [model["name"] for model in models.get("models", [])]
            
            model_available = any(MODEL_NAME in model for model in available_models)
            
            return {
                "status": "healthy" if model_available else "model_missing",
                "ollama_running": True,
                "model_name": MODEL_NAME,
                "model_available": model_available,
                "available_models": available_models
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": f"Cannot connect to Ollama: {str(e)}",
            "ollama_running": False
        }

@app.get("/files")
async def list_processed_files():
    """
    List all processed files with OCR and LLM results
    """
    try:
        files = []
        for results_file in OCR_RESULTS_DIR.glob("*_ocr_results.json"):
            file_id = results_file.stem.replace("_ocr_results", "")
            
            with open(results_file, "r", encoding="utf-8") as f:
                ocr_data = json.load(f)
            
            # Check if LLM results exist
            llm_file = PROCESSED_RESULTS_DIR / f"{file_id}_llm_results.json"
            llm_processed = llm_file.exists()
            
            files.append({
                "file_id": file_id,
                "total_pages": ocr_data["total_pages"],
                "total_words": ocr_data.get("total_words", 0),
                "processing_timestamp": ocr_data["processing_timestamp"],
                "llm_processed": llm_processed
            })
        
        return JSONResponse(content={"files": files})
        
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        raise HTTPException(status_code=500, detail="Error listing files")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)