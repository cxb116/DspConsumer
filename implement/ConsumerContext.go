package implement

//type ConsumerContext struct {
//	Msg     string
//	Cancel  context.CancelFunc
//	Context context.Context
//}
//
//var ConsumerContextPool = sync.Pool{
//	New: func() interface{} {
//		ctx := &ConsumerContext{}
//		// 初始化时创建永不过期的Context
//		ctx.Context, ctx.Cancel = context.WithCancel(context.Background())
//		return ctx
//	},
//}
//
//func GetConsumerContext() *ConsumerContext {
//	ctx := ConsumerContextPool.Get().(*ConsumerContext)
//
//	ctx.reset()
//	baseCtx := context.Background()
//	ctx.Context, ctx.Cancel = context.WithTimeout(baseCtx, 3*time.Second)
//
//	return ctx
//}
//
//func PutConsumerContext(ctx *ConsumerContext) {
//	if ctx == nil {
//		return
//	}
//
//	if ctx.Cancel != nil {
//		ctx.Cancel()
//		// 恢复为基础Context
//		ctx.Context, ctx.Cancel = context.WithCancel(context.Background())
//	}
//	// 回收
//	ctx.reset()
//	ConsumerContextPool.Put(ctx)
//}
//
//func (this *ConsumerContext) reset() {
//	this.Msg = ""
//}
