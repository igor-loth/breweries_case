FROM mageai/mageai:latest

# Definir diretório de trabalho
WORKDIR /app

# Copiar arquivos da aplicação para o contêiner
COPY . /app

# Instalar dependências
RUN pip install --no-cache-dir -r requirements.txt