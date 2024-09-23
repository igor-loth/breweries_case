# breweries case(Data Pipeline)

### Introdução
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
