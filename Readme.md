# 🛰️ ArcGIS ETL Pipeline

This project provides a modular ETL (Extract, Transform, Load) pipeline to process geospatial or tabular data and load it into an **ArcGIS Online Hosted Feature Layer**.

It supports:
- Reading data from a **CSV file** or an **external database**
- Cleaning and transforming the data
- Creating or updating a hosted feature layer in ArcGIS Online

---

## 📁 Project Structure

```.
├── README.md                  # Project documentation
├── run\_etl.py                # Entry point to run the ETL
├── requirements.txt          # Python dependencies
│
├── data/
│   └── sample\_data.csv       # Sample CSV input
│
├── notebooks/
│   └── notebook.ipynb        # Jupyter notebook for exploration/testing
│
├── etl\_pipeline/
│   ├── **init**.py
│   ├── pipeline.py           # Core ETL process
│   ├── schema.py             # Schema validation and field mapping
│   ├── config/
│   │   └── settings.yaml     # Config for DB connection, AGOL settings
│   └── database/
│       └── db.py             # Database connection and query logic

````

---

## ⚙️ Setup & Installation

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/arcgis-etl-pipeline.git
cd arcgis-etl-pipeline
````

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure the Pipeline

Edit the config file at: `etl_pipeline/config/settings.yaml`

```yaml
source:
  type: csv                         # "csv" or "db"
  path: data/sample_data.csv        # Used if type = csv
  db_connection:                    # Used if type = db
    dialect: postgresql
    host: localhost
    port: 5432
    database: my_database
    username: my_user
    password: my_password
    table: my_table

arcgis:
  username: your_agol_username
  password: your_agol_password
  layer_name: My_Feature_Layer
  folder: "ETL Outputs"
  geometry_type: point             # point, polygon, polyline
  spatial_reference: 4326
```

---

## 🚀 Running the ETL Pipeline

```bash
python run_etl.py
```

The pipeline will:

* Read the data from the configured source
* Clean and transform the dataset (e.g., geometry, missing values)
* Create or update a hosted feature layer on ArcGIS Online
* Upload the data as features

---

## 🧹 Data Transformation

During transformation, the pipeline performs:

* Column renaming or dropping
* Data type standardization
* Geometry validation (e.g., lat/lon to `Point` geometry)
* Schema validation against the ArcGIS field types

---

## 🧪 Development & Testing

Use the Jupyter notebook in `notebooks/` for:

* Testing field mappings
* Exploring data sources
* Debugging the transformation process

---

## 🗂️ Sample Data

* You can place your CSV files under `data/`
* For database input, update the connection in `settings.yaml`

---

## 📌 Requirements

* Python 3.8+
* ArcGIS Python API (`arcgis`)
* pandas, sqlalchemy, click, etc.

Install everything with:

```bash
pip install -r requirements.txt
```

---

## 📜 License

This project is licensed under the MIT License. Feel free to use, modify, and contribute.

---

## 🙌 Acknowledgments

Built for streamlined data integration with **ArcGIS Online**, using the ArcGIS Python API.
