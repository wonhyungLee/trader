import axios from 'axios';

// 상대 경로 /api 사용 (Nginx 프록시 설정 전제)
const baseURL = import.meta.env.VITE_API_BASE || '/api';
const api = axios.create({ baseURL });

export const fetchUniverse = () => api.get('/universe').then(r => r.data);
export const fetchPrices = (code, days = 60) => api.get('/prices', { params: { code, days } }).then(r => r.data);
export const fetchSignals = () => api.get('/signals').then(r => r.data);
export const fetchStatus = () => api.get('/status').then(r => r.data);
export const fetchEngines = () => api.get('/engines').then(r => r.data);
export const fetchOrders = () => api.get('/orders').then(r => r.data);
export const fetchPositions = () => api.get('/positions').then(r => r.data);
export const fetchStrategy = () => api.get('/strategy').then(r => r.data);
