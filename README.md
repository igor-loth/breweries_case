# breweries case(Data Pipeline)

## Introdução
O objetivo desse projeto é criar um pipeline consumindo dados de um API e armazenando em um ambiente Data Lake seguindo a arquitetura Medallion com três camadas:
- **Bronze 🟤:** Dados Brutos.
- **Silver ⚪:** Dados selecionados e particionado por localização.
- **Gold 🟡:** Dados agredados para análise.
- **Extra 📊:** Visualização gráfica dos dados que estão na camada Gold.

### Feramentas utilizadas 
- **Python** - Linguagem escolhida para a extração e tratamento dos dados entre as camadas do nosso Data Lake.
- **Mage** - Orquestrador do nosso ambiente, nele podemos ter uma visão macro do nosso pipeline e trabalhar com blocos (data loader, transform, data exporter).
- **MinIO** - Repositório de dados (Tem o mesmo conceito que o AWS S3). Nele vamos criar nossas camadas do Data Lake que armazenarão os dados.
- **Docker** - Conteinerização do nosso ambiente, configurado em um *docker-compose.yml*.

## Arquitetura
![GET](image/arquitetura.png)

## Pré Work
Caso queira ver o Pipeline funcionando, antes de clonar o repositório, tem alguns requisitos necessário: 
- Instalar Docker (Estou utilizando no linux).
- Instalar Python (Python 3.10.12 or more).
- Criar um arquivo *.env* para incluir as chaves de acesso do MinIO:
  ```bash
      # Definir o nome do usuário e senha que precisa conectar no serviço Web
      MINIO_ACCESS_KEY='<USER>'
      MINIO_SECRET_KEY='<SENHA>'
  ```
- Subir o ambiente:
  ```bash
      # Construir as imagens 
      docker-compose build

      # Subir o ambiente
      docker-compose up -d
  ```

## Extração - Bronze
Para a extração, foi criado um script python utilizando o bloco Data Loader do Mage [extract_breweries_data.py](data/data_loaders/extract_breweries_data.py) no qual captura os dados da API e armazena na camada Bronze no MinIO. O script salva os dados em um arquivo JSON ```breweries_raw.json``` na partição ```bronze/data/breweries/```. Caso o bucket não tenha sido criado, o próprio script faz a criação. 
![GET](image/arquitetura.png)

## Transformação - Silver
Para a extração, foi criado um script python utilizando o bloco Data Loader do Mage [extract_breweries_data.py](data/data_loaders/extract_breweries_data.py) no qual captura os dados da API e armazena na camada Bronze no MinIO. O script salva os dados em um arquivo JSON ```breweries_raw.json``` na partição ```bronze/data/breweries/```. Caso o bucket não tenha sido criado, o próprio script faz a criação. 
![GET](image/arquitetura.png)

## Transformação - Gold
Para a extração, foi criado um script python utilizando o bloco Data Loader do Mage [extract_breweries_data.py](data/data_loaders/extract_breweries_data.py) no qual captura os dados da API e armazena na camada Bronze no MinIO. O script salva os dados em um arquivo JSON ```breweries_raw.json``` na partição ```bronze/data/breweries/```. Caso o bucket não tenha sido criado, o próprio script faz a criação. 
![GET](image/arquitetura.png)

## Disponibilização - Data viz
Para a extração, foi criado um script python utilizando o bloco Data Loader do Mage [extract_breweries_data.py](data/data_loaders/extract_breweries_data.py) no qual captura os dados da API e armazena na camada Bronze no MinIO. O script salva os dados em um arquivo JSON ```breweries_raw.json``` na partição ```bronze/data/breweries/```. Caso o bucket não tenha sido criado, o próprio script faz a criação. 
![GET](image/arquitetura.png)


