CREATE DATABASE jardineria_staging;
USE jardineria_staging;

CREATE TABLE cliente (
    ID_cliente INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    nombre_cliente VARCHAR(50) NOT NULL,
    telefono VARCHAR(15) NOT NULL,
    ciudad VARCHAR(50) NOT NULL,
    region VARCHAR(50) DEFAULT NULL,
    pais VARCHAR(50) DEFAULT NULL,
    codigo_postal VARCHAR(10),
    categoria_cliente AS CASE 
        WHEN pais = 'Spain' THEN 'Europa'
        WHEN pais IN ('USA', 'Canada') THEN 'Norteamérica'
        ELSE 'Internacional'
    END PERSISTED,
    ultimo_cambio DATETIME DEFAULT GETDATE()
);

SET IDENTITY_INSERT cliente ON;
INSERT INTO cliente (ID_cliente, nombre_cliente, telefono, ciudad, region, pais, codigo_postal) 
SELECT ID_cliente, nombre_cliente, telefono, ciudad, region, pais, codigo_postal 
FROM jardineria.dbo.cliente;
SET IDENTITY_INSERT cliente OFF;

UPDATE cliente 
SET telefono = CASE 
    WHEN telefono LIKE '+%' THEN telefono
    WHEN LEN(telefono) = 9 AND pais = 'Spain' THEN '+34' + telefono
    WHEN LEN(telefono) = 10 AND pais = 'USA' THEN '+1' + telefono
    ELSE telefono
END,
ultimo_cambio = GETDATE();

CREATE TABLE producto (
    ID_producto INT IDENTITY(1,1) PRIMARY KEY,
    CodigoProducto VARCHAR(15) NOT NULL,
    nombre VARCHAR(70) NOT NULL,
    Categoria INT NOT NULL,
    cantidad_en_stock SMALLINT NOT NULL,
    proveedor VARCHAR(70) NOT NULL,
    precio_venta NUMERIC(15,2) NOT NULL,
    nivel_stock AS CASE 
        WHEN cantidad_en_stock <= 10 THEN 'Bajo'
        WHEN cantidad_en_stock BETWEEN 11 AND 50 THEN 'Medio'
        ELSE 'Alto'
    END PERSISTED,
    ultimo_cambio DATETIME DEFAULT GETDATE()
);

SET IDENTITY_INSERT producto ON;
INSERT INTO producto (ID_producto, CodigoProducto, nombre, Categoria, cantidad_en_stock, proveedor, precio_venta) 
SELECT ID_producto, CodigoProducto, nombre, Categoria, cantidad_en_stock, proveedor, precio_venta 
FROM jardineria.dbo.producto;
SET IDENTITY_INSERT producto OFF;

CREATE TABLE empleado (
    ID_empleado INT IDENTITY(1,1) PRIMARY KEY,
    nombre VARCHAR(50),
    apellido1 VARCHAR(50),
    extension VARCHAR(10),
    email VARCHAR(100),
    puesto VARCHAR(70),
    pais_oficina VARCHAR(50),
    ciudad_oficina VARCHAR(30),
    ultimo_cambio DATETIME DEFAULT GETDATE()
);

SET IDENTITY_INSERT empleado ON;
INSERT INTO empleado (ID_empleado, nombre, apellido1, extension, email, puesto, pais_oficina, ciudad_oficina)
SELECT 
    e.ID_empleado, 
    e.nombre, 
    e.apellido1, 
    e.extension, 
    e.email, 
    e.puesto,
    o.pais,
    o.ciudad
FROM jardineria.dbo.empleado e
INNER JOIN jardineria.dbo.oficina o ON e.ID_oficina = o.ID_oficina;
SET IDENTITY_INSERT empleado OFF;

CREATE TABLE fecha (
    ID_fecha INT PRIMARY KEY,
    fecha_pedido DATE NOT NULL,
    fecha_esperada DATE NOT NULL,
    fecha_entrega DATE,
    año_pedido AS YEAR(fecha_pedido) PERSISTED,
    mes_pedido AS MONTH(fecha_pedido) PERSISTED,
    trimestre_pedido AS DATEPART(QUARTER, fecha_pedido) PERSISTED,
    dias_entrega AS CASE 
        WHEN fecha_entrega IS NOT NULL THEN DATEDIFF(DAY, fecha_pedido, fecha_entrega) 
        ELSE NULL 
    END PERSISTED,
    estado_entrega AS CASE
        WHEN fecha_entrega IS NULL THEN 'Pendiente'
        WHEN fecha_entrega <= fecha_esperada THEN 'A tiempo'
        ELSE 'Retrasado'
    END PERSISTED,
    ultimo_cambio DATETIME DEFAULT GETDATE()
);

INSERT INTO fecha (ID_fecha, fecha_pedido, fecha_esperada, fecha_entrega)
SELECT 
    ROW_NUMBER() OVER (ORDER BY fecha_pedido),
    fecha_pedido,
    fecha_esperada,
    fecha_entrega
FROM jardineria.dbo.pedido
GROUP BY fecha_pedido, fecha_esperada, fecha_entrega;

CREATE TABLE ventas (
    ID_cliente INT NOT NULL,
    ID_producto INT NOT NULL,
    ID_empleado INT NOT NULL,
    ID_fecha INT NOT NULL,
    /* CAMBIO 5: Añadir información de cantidad y precio para análisis */
    cantidad INT NOT NULL,
    precio_unidad NUMERIC(15,2) NOT NULL,
    total_linea AS (cantidad * precio_unidad) PERSISTED,
    /* CAMBIO 6: Añadir columna para registro de último cambio */
    ultimo_cambio DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ID_cliente) REFERENCES cliente(ID_cliente),
    FOREIGN KEY (ID_producto) REFERENCES producto(ID_producto),
    FOREIGN KEY (ID_empleado) REFERENCES empleado(ID_empleado),
    FOREIGN KEY (ID_fecha) REFERENCES fecha(ID_fecha)
);

-- Insertar datos en la tabla ventas (modificado para incluir cantidad y precio)
INSERT INTO ventas (ID_cliente, ID_producto, ID_empleado, ID_fecha, cantidad, precio_unidad)
SELECT 
    p.ID_cliente,
    dp.ID_producto,
    c.ID_empleado_rep_ventas,
    f.ID_fecha,
    dp.cantidad,
    dp.precio_unidad
FROM jardineria.dbo.pedido p
INNER JOIN jardineria.dbo.detalle_pedido dp ON p.ID_pedido = dp.ID_pedido
INNER JOIN jardineria.dbo.cliente c ON p.ID_cliente = c.ID_cliente
INNER JOIN fecha f ON (
    p.fecha_pedido = f.fecha_pedido AND 
    p.fecha_esperada = f.fecha_esperada AND 
    (p.fecha_entrega = f.fecha_entrega OR (p.fecha_entrega IS NULL AND f.fecha_entrega IS NULL))
)
WHERE c.ID_empleado_rep_ventas IS NOT NULL;

CREATE VIEW vista_ventas_ordenada AS
SELECT 
    v.ID_cliente,
    c.nombre_cliente,
    c.categoria_cliente,
    v.ID_producto,
    p.nombre AS nombre_producto,
    p.nivel_stock,
    v.ID_empleado,
    e.nombre + ' ' + e.apellido1 AS nombre_empleado,
    v.ID_fecha,
    f.fecha_pedido,
    f.año_pedido,
    f.mes_pedido,
    f.trimestre_pedido,
    f.fecha_entrega,
    f.estado_entrega,
    f.dias_entrega,
    v.cantidad,
    v.precio_unidad,
    v.total_linea,
    v.ultimo_cambio AS ultimo_cambio_ventas,
    c.ultimo_cambio AS ultimo_cambio_cliente,
    p.ultimo_cambio AS ultimo_cambio_producto,
    e.ultimo_cambio AS ultimo_cambio_empleado,
    f.ultimo_cambio AS ultimo_cambio_fecha
FROM ventas v
INNER JOIN cliente c ON v.ID_cliente = c.ID_cliente
INNER JOIN producto p ON v.ID_producto = p.ID_producto
INNER JOIN empleado e ON v.ID_empleado = e.ID_empleado
INNER JOIN fecha f ON v.ID_fecha = f.ID_fecha;

-- Consultar la vista
SELECT * FROM vista_ventas_ordenada;