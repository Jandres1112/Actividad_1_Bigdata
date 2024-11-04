

CREATE TABLE IF NOT EXISTS house_price.propiedad
(
    
    id_propiedad SERIAL PRIMARY KEY,
    Avg_Area_Income BIGINT,
    House_Age BIGINT,
    Number_of_Rooms BIGINT,
    Number_of_Bedrooms BIGINT,
    Area_Population BIGINT,
    Price BIGINT,    
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
    
    
);


