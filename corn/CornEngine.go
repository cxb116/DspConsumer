package corn

//type JobFunc func()
//
//type Manager struct {
//	cron     *cron.Cron
//	jobs     map[string]cron.EntryID
//	jobsFunc map[string]JobFunc
//	mutex    sync.RWMutex
//}
//
//func NewManager() *Manager {
//	return &Manager{
//		cron:     cron.New(cron.WithSeconds()),
//		jobs:     make(map[string]cron.EntryID),
//		jobsFunc: make(map[string]JobFunc),
//	}
//}
//
//func (m *Manager) Init() {
//	m.registerJobs()
//}
//
//func (m *Manager) RegisterJob(key string, job JobFunc) {
//	m.mutex.Lock()
//	defer m.mutex.Unlock()
//	m.jobsFunc[key] = job
//}
//
//func (m *Manager) Start() {
//	//cron 表达式
//	hardcodedCron := map[string]string{
//
//		//"CronExcSlotDay":  "0 0,10,20,30,40,50 * * * *",
//		//"CronExcSlotHour": "0 0,10,20,30,40,50 * * *",
//	}
//
//	for key, expr := range hardcodedCron {
//		jobFunc, ok := m.jobsFunc[key]
//		if !ok {
//			continue
//		}
//
//		entryID, err := m.cron.AddFunc(expr, m.wrapJob(key, jobFunc))
//		if err != nil {
//			logger.SystemLog.Error().Msgf("cron.AddFunc err: %v, job key: %s", err, key)
//			panic(err)
//		}
//
//		m.mutex.Lock()
//		m.jobs[key] = entryID
//		m.mutex.Unlock()
//	}
//
//	m.cron.Start()
//	logger.SystemLog.Info().Msgf("cron service started")
//}
//
//func (m *Manager) Stop() {
//	if m.cron != nil {
//		m.cron.Stop()
//	}
//	logger.SystemLog.Info().Msgf("cron service stopped")
//}
//
//func (m *Manager) UpdateJobSchedule(key, expr string) error {
//	m.mutex.Lock()
//	defer m.mutex.Unlock()
//
//	jobFunc, ok := m.jobsFunc[key]
//	if !ok {
//		return fmt.Errorf("job not found: %s", key)
//	}
//
//	if oldID, exists := m.jobs[key]; exists {
//		m.cron.Remove(oldID)
//	}
//
//	entryID, err := m.cron.AddFunc(expr, m.wrapJob(key, jobFunc))
//	if err != nil {
//		return err
//	}
//
//	m.jobs[key] = entryID
//	return nil
//}
//
//func (m *Manager) wrapJob(key string, job JobFunc) JobFunc {
//	return func() {
//		defer func() {
//			if r := recover(); r != nil {
//				var buf bytes.Buffer
//				stack := make([]byte, 4096)
//				n := runtime.Stack(stack, false)
//
//				buf.WriteString(fmt.Sprintf(
//					"[cron:%s] panic: %v\n", key, r,
//				))
//				buf.Write(stack[:n])
//
//				logger.SystemLog.Info().Msg(buf.String())
//			}
//		}()
//
//		start := time.Now()
//		job()
//		logger.SystemLog.Info().Msgf("[cron:%s] finished, cost=%v\n", key, time.Since(start))
//	}
//}
//
//// 注册
//func (m *Manager) registerJobs() {
//
//	//m.RegisterJob("CronExcSlotDay", func() {
//	//	logger.SystemLog.Error().Msgf(" 11111111111111111111111111111111111111111111111111 秒执行1次")
//	//	err := CronExcSlotDay()
//	//	if err != nil {
//	//		logger.SystemLog.Error().Msgf("CronExceDspSlotDay err: %v", err)
//	//	}
//	//})
//	//
//	//m.RegisterJob("ExcDspSlotHour", func() {
//	//	logger.SystemLog.Error().Msgf(" 3 秒执行1次")
//	//	err := CronExcSlotHour()
//	//	if err != nil {
//	//		logger.SystemLog.Error().Msgf("CronExceDspSlotDay err: %v", err)
//	//	}
//	//})
//
//}
