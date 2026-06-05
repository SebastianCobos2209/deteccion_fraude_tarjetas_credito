// ================================================================
// mongo-init/01_init_schema.js
// Script de inicialización de MongoDB para el TFM de fraude financiero
// Configurado con 6 colecciones: user_profiles, cards, 
// transactions_raw, transactions_enriched, fraud_alerts, model_metrics.
// ================================================================

db = db.getSiblingDB('fraude_db');

print('Iniciando configuración del esquema de base de datos...');

// ================================================================
// COLECCIÓN 1: user_profiles (Datos estáticos de usuarios)
// ================================================================
db.createCollection('user_profiles', {
  validationLevel: 'moderate',
  validationAction: 'warn', // Gobierno de Datos: Advertir en logs en lugar de rechazar transacción en streaming
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['usuarioID'],
      properties: {
        usuarioID: { bsonType: 'string', description: 'UUID único del usuario' },
        usuario: { bsonType: 'string' },
        email: { bsonType: 'string' },
        fecha_nacimiento: { bsonType: 'string' },
        usuario_x: { bsonType: 'double' },
        usuario_y: { bsonType: 'double' },
        addr1: { bsonType: 'double' },
        addr2: { bsonType: 'double' },
        DeviceType: { bsonType: 'string' },
        DeviceInfo: { bsonType: 'string' },
        promedio_de_gastos: { bsonType: 'double' },
        varianza_de_gastos: { bsonType: 'double' },
        promedio_gastos_por_dia: { bsonType: 'double' },
        vertexon_customer_number: { bsonType: 'string' }
      }
    }
  }
});
db.user_profiles.createIndex({ usuarioID: 1 }, { unique: true });
print('-> Colección user_profiles creada con índice único.');

// ================================================================
// COLECCIÓN 2: cards (Datos estáticos de tarjetas)
// ================================================================
db.createCollection('cards', {
  validationLevel: 'moderate',
  validationAction: 'warn',
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['tarjetaID', 'usuarioID'],
      properties: {
        tarjetaID: { bsonType: 'string', description: 'ID de la tarjeta' },
        usuarioID: { bsonType: 'string', description: 'ID del propietario' },
        card_number_masked: { bsonType: 'string' },
        fecha_exp_tarjeta: { bsonType: 'string' },
        cvv: { bsonType: 'string', description: 'Encriptado Fernet o RSA' },
        card1: { bsonType: 'int' },
        card4: { bsonType: 'string' },
        card6: { bsonType: 'string' },
        ProductCD: { bsonType: 'string' }
      }
    }
  }
});
db.cards.createIndex({ tarjetaID: 1 }, { unique: true });
db.cards.createIndex({ usuarioID: 1 });
print('-> Colección cards creada con índices.');

// ================================================================
// COLECCIÓN 3: transactions_raw (Eventos crudos desde Kafka)
// ================================================================
db.createCollection('transactions_raw', {
  validationLevel: 'moderate',
  validationAction: 'warn',
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['transaccionID', 'usuarioID', 'tarjetaID', 'fecha_hora_transaccion', 'TransactionAmt'],
      properties: {
        transaccionID: { bsonType: 'string' },
        usuarioID: { bsonType: 'string' },
        tarjetaID: { bsonType: 'string' },
        fecha_hora_transaccion: { bsonType: 'date' },
        TransactionAmt: { bsonType: 'double' },
        TransactionDT: { bsonType: ['int', 'long'] },
        isFraud: { bsonType: 'int', enum: [0, 1] }
      }
    }
  }
});
db.transactions_raw.createIndex({ transaccionID: 1 }, { unique: true });
db.transactions_raw.createIndex({ usuarioID: 1, fecha_hora_transaccion: -1 });
// TTL Index: Limpieza automática a los 30 días (30 * 24 * 60 * 60 = 2592000 segundos)
db.transactions_raw.createIndex({ fecha_hora_transaccion: 1 }, { expireAfterSeconds: 2592000 });
print('-> Colección transactions_raw creada con índice compuesto y TTL de 30 días.');

// ================================================================
// COLECCIÓN 4: transactions_enriched (Variables + Inferencia desnormalizadas)
// ================================================================
db.createCollection('transactions_enriched', {
  validationLevel: 'moderate',
  validationAction: 'warn',
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['transaccionID', 'usuarioID', 'tarjetaID', 'processed_at', 'fraud_score'],
      properties: {
        transaccionID: { bsonType: 'string' },
        usuarioID: { bsonType: 'string' },
        tarjetaID: { bsonType: 'string' },
        
        // Variables desnormalizadas
        TransactionAmt: { bsonType: 'double' },
        TransactionDT: { bsonType: ['int', 'long'] },
        card1: { bsonType: 'int' },
        card4: { bsonType: 'string' },
        card6: { bsonType: 'string' },
        ProductCD: { bsonType: 'string' },
        P_emaildomain: { bsonType: 'string' },
        addr1: { bsonType: 'double' },
        addr2: { bsonType: 'double' },
        DeviceType: { bsonType: 'string' },
        DeviceInfo: { bsonType: 'string' },
        
        // Controles C y delta D
        C1: { bsonType: 'double' },
        C13: { bsonType: 'double' },
        C7: { bsonType: 'double' },
        C14: { bsonType: 'double' },
        D1: { bsonType: 'double' },
        
        // Variables V
        V314: { bsonType: 'double' },
        V201: { bsonType: 'double' },
        V243: { bsonType: 'double' },
        V257: { bsonType: 'double' },
        V242: { bsonType: 'double' },
        V45: { bsonType: 'double' },
        V246: { bsonType: 'double' },
        V200: { bsonType: 'double' },
        V258: { bsonType: 'double' },
        
        // Enriquecidos por Spark
        zscore_amt: { bsonType: 'double' },
        velocity: { bsonType: 'double' },
        amt_distance: { bsonType: 'double' },
        
        // Predicción e Inferencia
        fraud_score: { bsonType: 'double' },
        is_suspicious: { bsonType: 'bool' },
        model_ready: { bsonType: 'bool' },
        processed_at: { bsonType: 'date' }
      }
    }
  }
});
db.transactions_enriched.createIndex({ transaccionID: 1 }, { unique: true });
// Índice clave de Grafana: Ordenar alertas de fraude por score e historial temporal
db.transactions_enriched.createIndex({ fraud_score: -1, processed_at: -1 });
// Índices de análisis y desagregación sin lookup para el dashboard
db.transactions_enriched.createIndex({ card4: 1, processed_at: -1 });
db.transactions_enriched.createIndex({ DeviceType: 1, processed_at: -1 });
db.transactions_enriched.createIndex({ P_emaildomain: 1, processed_at: -1 });
// TTL Index: Limpieza automática a los 90 días (90 * 24 * 60 * 60 = 7776000 segundos)
db.transactions_enriched.createIndex({ processed_at: 1 }, { expireAfterSeconds: 7776000 });
print('-> Colección transactions_enriched creada con índices compuestos y TTL de 90 días.');

// ================================================================
// COLECCIÓN 5: fraud_alerts (Bandeja operativa de sospechas)
// ================================================================
db.createCollection('fraud_alerts', {
  validationLevel: 'moderate',
  validationAction: 'warn',
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['transaccionID', 'alerted_at', 'fraud_score'],
      properties: {
        transaccionID: { bsonType: 'string' },
        usuarioID: { bsonType: 'string' },
        tarjetaID: { bsonType: 'string' },
        TransactionAmt: { bsonType: 'double' },
        fraud_score: { bsonType: 'double' },
        zscore_amt: { bsonType: 'double' },
        velocity: { bsonType: 'double' },
        isFraud_label: { bsonType: 'int', enum: [0, 1] },
        alerted_at: { bsonType: 'date' },
        status: { bsonType: 'string', enum: ['flagged', 'blocked', 'confirmed_fraud', 'dismissed'] },
        reviewed_by: { bsonType: ['string', 'null'] },
        reviewed_at: { bsonType: ['date', 'null'] }
      }
    }
  }
});
db.fraud_alerts.createIndex({ transaccionID: 1 }, { unique: true });
db.fraud_alerts.createIndex({ alerted_at: -1 });
db.fraud_alerts.createIndex({ fraud_score: -1 });
// TTL Index: Limpieza automática a los 90 días (90 * 24 * 60 * 60 = 7776000 segundos)
db.fraud_alerts.createIndex({ alerted_at: 1 }, { expireAfterSeconds: 7776000 });
print('-> Colección fraud_alerts creada con TTL de 90 días.');

// ================================================================
// COLECCIÓN 6: model_metrics (Auditoría de IA y desempeño Batch)
// ================================================================
db.createCollection('model_metrics', {
  validationLevel: 'moderate',
  validationAction: 'warn',
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['evaluated_at', 'model_version'],
      properties: {
        evaluated_at: { bsonType: 'date' },
        samples_evaluated: { bsonType: 'int' },
        threshold: { bsonType: 'double' },
        auc_roc: { bsonType: 'double' },
        f1_score: { bsonType: 'double' },
        recall: { bsonType: 'double' },
        precision: { bsonType: 'double' },
        true_positives: { bsonType: 'int' },
        false_positives: { bsonType: 'int' },
        true_negatives: { bsonType: 'int' },
        false_negatives: { bsonType: 'int' },
        model_version: { bsonType: 'string' },
        contamination_rate: { bsonType: 'double' }
      }
    }
  }
});
db.model_metrics.createIndex({ evaluated_at: -1 });
print('-> Colección model_metrics creada.');

print('================================================================');
print('Base de datos fraude_db inicializada con el nuevo diseño Top 25.');
print('Colecciones disponibles: user_profiles, cards, transactions_raw, transactions_enriched, fraud_alerts, model_metrics.');
print('================================================================');