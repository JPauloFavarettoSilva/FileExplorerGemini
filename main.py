from bson import ObjectId
from fastapi import FastAPI, HTTPException, File, UploadFile, Depends
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import google.generativeai as genai
import xml.etree.ElementTree as ET
import pandas as pd
import json
import io
from contextlib import contextmanager
import uvicorn  # Importando o Uvicorn
from motor.motor_asyncio import AsyncIOMotorClient
from typing import AsyncGenerator
import bson.json_util as json_util


# Configuração do FastAPI
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client: AsyncIOMotorClient = None

@app.on_event("startup")
async def startup_db():
    global client
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    print("MongoDB connected!")

@app.on_event("shutdown")
async def shutdown_db():
    global client
    if client:
        client.close()
        print("MongoDB connection closed!")

# Configuração da API Gemini
genai.configure(api_key="CHAVE_DA_API")

def json_converter(obj):
        return str(obj)

# Função para gerar um JSON "pretty" sem barras invertidas ou caracteres especiais
def json_pretty_clean(obj):
    # Gera o JSON "pretty" com indentação
    json_str = json.dumps(obj, default=json_converter, indent=2)
    
    json_str = json_str.replace('\\\"', '"')  # Remove todas as barras invertidas (escape)
    # Remove as barras invertidas (escape) que podem aparecer em strings
    return json_str

# Função para extrair metadados do arquivo
async def extract_metadata(file: UploadFile):
    formato = file.filename.split('.')[-1].lower()
    tamanho = len(await file.read())  # Tamanho em bytes
    data_de_criacao = datetime.utcnow().isoformat() + "Z"

    metadados = {
        "nomeDoArquivo": file.filename,
        "formatoDoArquivo": formato,
        "tamanhoDoArquivo": tamanho,
        "dataDeCriacao": data_de_criacao
    }

    return metadados

# Função para processar o conteúdo do arquivo
async def process_file(file: UploadFile):
    content = await file.read()
    formato = file.filename.split('.')[-1].lower()

    if formato == "xml":
        root = ET.fromstring(content)
        conteudo = ET.tostring(root, encoding='utf-8').decode('utf-8') # Amostra de 500 caracteres

    elif formato == "csv":
        df = pd.read_csv(io.BytesIO(content))
        conteudo = df.head().to_json(orient="records")  # Primeiras linhas como amostra

    elif formato == "json":
        conteudo = json.dumps(json.loads(content.decode('utf-8')))  # Amostra dos 5 primeiros itens

    elif formato == "txt":
        conteudo = content.decode('utf-8')  # Amostra de 500 caracteres

    else:
        raise HTTPException(status_code=400, detail="Formato de arquivo não suportado.")

    return conteudo

# Função para enviar os metadados e amostra para a API do Gemini
async def send_to_gemini(metadados, amostra):
    prompt = f"Analise os seguintes metadados e conteúdo de amostra de um arquivo:\nMetadados: {metadados}\nAmostra: {amostra}\nEntregue um resumo em formato JSON:\nResponda somente com o JSON e não adicione nenhuma quebra de linha a resposta."
    response = genai.GenerativeModel("gemini-1.5-flash")
    result = response.generate_content([prompt])

    return result.text

# Endpoint para upload de arquivo
@app.post("/upload-file/")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Verificar o tipo do arquivo
        if not file.content_type in ['text/xml', 'text/csv', 'application/json', 'text/plain']:
            raise HTTPException(status_code=400, detail="Tipo de arquivo não suportado.")

        # Extrair metadados
        metadados = await extract_metadata(file)

        await file.seek(0)

        # Processar o conteúdo do arquivo (amostra)
        amostra_conteudo = await process_file(file)

        # Enviar metadados e amostra para a API do Gemini
        resumo_gemini = await send_to_gemini(metadados, amostra_conteudo)

        a = json.loads(resumo_gemini)

        # Criar o JSON de resposta
        resposta = {
            "metadados": metadados,
            "resumoGemini": a
        }

        x = resposta

        db = client["APIGemini"]
        await db["arquivos"].insert_one(x)
        
        y = (json_util.dumps(resposta, sort_keys=True, indent=4))
        return y

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o upload: {str(e)}")


# Rodar o servidor com Uvicorn diretamente
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
