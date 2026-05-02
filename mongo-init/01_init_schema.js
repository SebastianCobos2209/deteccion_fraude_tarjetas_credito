// ================================================================
// mongo-init/01_init_schema.js
// Script de inicialización de MongoDB para el TFM de fraude financiero
// Se ejecuta automáticamente al levantar el contenedor por primera vez
// ================================================================

// Seleccionar la base de datos
db = db.getSiblingDB('fraude_db');

// ================================================================
// COLECCIÓN 1: transactions
// Almacena cada transacción financiera recibida desde Kafka
// ================================================================
db.createCollection('transactions', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['transaction_id', 'user_id', 'amount', 'timestamp', 'status'],
      properties: {

        // ── Identificadores ──────────────────────────
        transaction_id: {
          bsonType: 'string',
          description: 'ID único de la transacción (requerido)'
        },
        user_id: {
          bsonType: 'string',
          description: 'ID del usuario que realiza la transacción (requerido)'
        },

        // ── Datos de la transacción ───────────────────
        amount: {
          bsonType: 'double',
          minimum: 0,
          description: 'Monto de la transacción en USD (requerido, >= 0)'
        },
        currency: {
          bsonType: 'string',
          enum: ['USD', 'EUR', 'GBP', 'MXN', 'COP', 'BRL'],
          description: 'Moneda de la transacción'
        },
        transaction_type: {
          bsonType: 'string',
          enum: ['card_present', 'card_not_present', 'transfer', 'atm', 'online'],
          description: 'Canal / tipo de transacción'
        },
        merchant_category: {
          bsonType: 'string',
          description: 'Categoría del comercio (MCC code): ej. grocery, travel, entertainment'
        },
        merchant_id: {
          bsonType: 'string',
          description: 'ID del comercio donde ocurrió la transacción'
        },

        // ── Tiempo ────────────────────────────────────
        timestamp: {
          bsonType: 'date',
          description: 'Fecha y hora de la transacción en UTC (requerido)'
        },
        hour_of_day: {
          bsonType: 'int',
          minimum: 0,
          maximum: 23,
          description: 'Hora del día (0-23) — feature para ML'
        },
        day_of_week: {
          bsonType: 'int',
          minimum: 0,
          maximum: 6,
          description: 'Día de la semana (0=lunes, 6=domingo) — feature para ML'
        },

        // ── Geolocalización ───────────────────────────
        location: {
          bsonType: 'object',
          properties: {
            country: { bsonType: 'string' },
            city: { bsonType: 'string' },
            lat: { bsonType: 'double' },
            lon: { bsonType: 'double' }
          }
        },
        ip_address: {
          bsonType: 'string',
          description: 'IP desde donde se originó la transacción (enmascarada por GDPR)'
        },

        // ── Features calculadas por PySpark ───────────
        // Estos campos los enriquece Sebastian en el pipeline de Spark
        features: {
          bsonType: 'object',
          properties: {
            txn_velocity_1min: {
              bsonType: 'int',
              description: 'Nº de transacciones del usuario en el último minuto'
            },
            txn_velocity_5min: {
              bsonType: 'int',
              description: 'Nº de transacciones del usuario en los últimos 5 minutos'
            },
            txn_velocity_1hour: {
              bsonType: 'int',
              description: 'Nº de transacciones del usuario en la última hora'
            },
            delta_amount: {
              bsonType: 'double',
              description: 'Desviación del monto respecto al monto medio histórico del usuario'
            },
            amount_zscore: {
              bsonType: 'double',
              description: 'Z-score del monto frente al perfil histórico del usuario'
            },
            geo_anomaly: {
              bsonType: 'bool',
              description: 'True si la ubicación es inusual para este usuario'
            },
            time_since_last_txn_sec: {
              bsonType: 'double',
              description: 'Segundos desde la última transacción del mismo usuario'
            }
          }
        },

        // ── Resultado del modelo ───────────────────────
        fraud_score: {
          bsonType: 'double',
          minimum: 0.0,
          maximum: 1.0,
          description: 'Score de fraude del ensemble (0=legítimo, 1=fraude)'
        },
        is_fraud_predicted: {
          bsonType: 'bool',
          description: 'Clasificación final del modelo (True = fraude detectado)'
        },
        is_fraud_confirmed: {
          bsonType: ['bool', 'null'],
          description: 'Etiqueta real confirmada por analista (null = sin revisar)'
        },

        // ── Estado ────────────────────────────────────
        status: {
          bsonType: 'string',
          enum: ['pending', 'processed', 'flagged', 'blocked', 'confirmed_fraud', 'confirmed_legitimate'],
          description: 'Estado del procesamiento de la transacción (requerido)'
        },
        processing_time_ms: {
          bsonType: 'double',
          description: 'Tiempo total de procesamiento en milisegundos (KPI de latencia)'
        }
      }
    }
  }
});

// Índices para optimizar queries frecuentes
db.transactions.createIndex({ transaction_id: 1 }, { unique: true });
db.transactions.createIndex({ user_id: 1, timestamp: -1 });        // historial por usuario
db.transactions.createIndex({ timestamp: -1 });                      // queries temporales
db.transactions.createIndex({ is_fraud_predicted: 1, timestamp: -1 }); // alertas de fraude
db.transactions.createIndex({ status: 1 });                          // filtrado por estado
db.transactions.createIndex({ fraud_score: -1 });                    // ranking por score

print('Colección transactions creada con índices');

// ================================================================
// COLECCIÓN 2: user_profiles
// Perfil histórico de cada usuario — Rosa la alimenta,
// Sebastian la consulta para enriquecer transacciones en streaming
// ================================================================
db.createCollection('user_profiles');

db.user_profiles.createIndex({ user_id: 1 }, { unique: true });

// Documento de ejemplo para guiar el schema
db.user_profiles.insertOne({
  user_id: 'USR_EXAMPLE_001',
  created_at: new Date(),
  updated_at: new Date(),

  // Estadísticas calculadas sobre el historial
  stats: {
    total_transactions: 0,
    avg_amount: 0.0,
    std_amount: 0.0,
    max_amount: 0.0,
    typical_hour_range: [8, 22],        // horas habituales de actividad
    typical_countries: ['EC', 'US'],    // países habituales
    typical_merchant_categories: [],
    fraud_history_count: 0
  },

  // Ventana deslizante — últimas N transacciones (para LSTM)
  recent_transactions: [],              // array de los últimos 10 transaction_id

  // Flags de riesgo
  risk_flags: {
    account_age_days: 0,
    has_previous_fraud: false,
    account_takeover_risk: false
  }
});

print('Colección user_profiles creada con índices');

// ================================================================
// COLECCIÓN 3: fraud_alerts
// Registro de todas las alertas generadas
// Para auditoría y reentrenamiento del modelo
// ================================================================
db.createCollection('fraud_alerts');

db.fraud_alerts.createIndex({ transaction_id: 1 }, { unique: true });
db.fraud_alerts.createIndex({ created_at: -1 });
db.fraud_alerts.createIndex({ reviewed: 1, created_at: -1 });

print('Colección fraud_alerts creada con índices');

// ================================================================
// COLECCIÓN 4: model_metrics
// Registro de métricas por versión de modelo (para el Cap. 5 del TFM)
// ================================================================
db.createCollection('model_metrics');

print('Colección model_metrics creada');

print('');
print('Base de datos fraude_db inicializada correctamente');
print('Colecciones: transactions, user_profiles, fraud_alerts, model_metrics');