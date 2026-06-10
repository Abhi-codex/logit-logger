import mongoose, { Schema, Document } from 'mongoose';

export interface ILog {
  id: string;
  matchId: string;
  timestamp: number;
  responseSentAt: number;
  appName: string;
  level: string;
  method: string;
  url: string;
  status: string | number;
  ip: string;
  content_length: string | number;
  response_time: number;
  message: string;
  metadata?: Record<string, any>;
}

const LogSchema = new Schema<ILog>({
  id: { type: String, required: true, unique: true },
  matchId: { type: String, required: true, index: true },
  timestamp: { type: Number, required: true, index: true },
  responseSentAt: { type: Number, required: true },
  appName: { type: String, required: true, index: true },
  level: { type: String, required: true, index: true },
  method: { type: String, default: '' },
  url: { type: String, default: '' },
  status: { type: Schema.Types.Mixed, default: 200 },
  ip: { type: String, default: '' },
  content_length: { type: Schema.Types.Mixed, default: 0 },
  response_time: { type: Number, default: 0 },
  message: { type: String, required: true },
  metadata: { type: Schema.Types.Mixed, default: {} }
});

export const LogModel = mongoose.model<ILog>('Log', LogSchema);

export async function connectDatabase(mongoUri: string): Promise<void> {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect(mongoUri);
    console.log('[Logit Database] Connected to MongoDB successfully.');
  }
}

export async function pruneLogs(maxCount: number): Promise<number> {
  try {
    const count = await LogModel.countDocuments();
    if (count > maxCount) {
      const excess = count - maxCount;
      const oldestLogs = await LogModel.find({})
        .sort({ timestamp: 1 })
        .limit(excess)
        .select('_id')
        .exec();
      
      const idsToDelete = oldestLogs.map(log => log._id);
      const result = await LogModel.deleteMany({ _id: { $in: idsToDelete } });
      console.log(`[Logit Database] Limit reached. Pruned ${result.deletedCount} oldest logs.`);
      return result.deletedCount || 0;
    }
  } catch (err) {
    console.error('[Logit Database] Error pruning logs:', err);
  }
  return 0;
}
