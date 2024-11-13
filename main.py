from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime
import google.generativeai as genai
import xml.etree.ElementTree as ET
import pandas as pd
import json
import io
import os

# Configuração do FastAPI
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexão com o MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["APIGemini"]
collection = db["arquivos"]

# Configuração da API Gemini
genai.configure(api_key="AIzaSyDQ0qwyrZYbir297YmjI6031YVO_NBAzp4")

# Função para extrair metadados do arquivo
async def extract_metadata(file: UploadFile):
    try:
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

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao extrair metadados: {str(e)}")

# Função para processar o conteúdo do arquivo
async def process_file(file: UploadFile):
    try:
        content = await file.read()
        formato = file.filename.split('.')[-1].lower()

        if formato == "xml":
            root = ET.fromstring(content)
            conteudo = ET.tostring(root, encoding='utf-8').decode('utf-8')[:500]  # Amostra de 500 caracteres

        elif formato == "csv":
            df = pd.read_csv(io.BytesIO(content))
            conteudo = df.head().to_json(orient="records")  # Primeiras linhas como amostra

        elif formato == "json":
            conteudo = json.dumps(json.loads(content.decode('utf-8'))[:5])  # Amostra dos 5 primeiros itens

        elif formato == "txt":
            conteudo = content.decode('utf-8')[:500]  # Amostra de 500 caracteres

        else:
            raise HTTPException(status_code=400, detail="Formato de arquivo não suportado.")

        return conteudo

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o arquivo: {str(e)}")

# Função para enviar os metadados e amostra para a API do Gemini
async def send_to_gemini(metadados, amostra):
    try:
        prompt = f"Analise os seguintes metadados e conteúdo de amostra de um arquivo:\nMetadados: {metadados}\nAmostra: {amostra}\nEntregue um resumo em formato JSON:"
        response = genai.GenerativeModel("gemini-1.5-flash")
        result = response.generate_content([prompt])

        return result.text

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao enviar para a API Gemini: {str(e)}")

# Endpoint para upload de arquivo
@app.post("/upload-file/")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Verificar o tipo do arquivo
        if not file.content_type in ['text/xml', 'text/csv', 'application/json', 'text/plain']:
            raise HTTPException(status_code=400, detail="Tipo de arquivo não suportado.")

        # Extrair metadados
        metadados = await extract_metadata(file)

        # Processar o conteúdo do arquivo (amostra)
        amostra_conteudo = await process_file(file)

        # Enviar metadados e amostra para a API do Gemini
        resposta_gemini = await send_to_gemini(metadados, amostra_conteudo)

        # Criar o JSON de resposta
        resposta = {
            "metadados": metadados,
            "resumoGemini": resposta_gemini
        }

        # Salvar no MongoDB
        collection.insert_one(resposta)

        return resposta

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar o upload: {str(e)}")
