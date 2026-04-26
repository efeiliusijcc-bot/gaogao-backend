import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ReportList } from './pages/ReportList';
import { NewReport } from './pages/NewReport';
import { ReportDetail } from './pages/ReportDetail';
import './App.css';

function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <header className="app-header">
          <nav>
            <a href="/reports" className="brand">OpenClaw 报告生成平台</a>
          </nav>
        </header>
        <main className="app-main">
          <Routes>
            <Route path="/reports" element={<ReportList />} />
            <Route path="/reports/new" element={<NewReport />} />
            <Route path="/reports/:jobId" element={<ReportDetail />} />
            <Route path="*" element={<Navigate to="/reports" replace />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;
