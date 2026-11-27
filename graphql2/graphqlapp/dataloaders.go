package graphqlapp

import (
	context "context"

	"github.com/google/uuid"
	"github.com/target/goalert/alert"
	"github.com/target/goalert/dataloader"
	"github.com/target/goalert/escalation"
	"github.com/target/goalert/gadb"
	"github.com/target/goalert/notification"
	"github.com/target/goalert/notificationchannel"
	"github.com/target/goalert/permission"
	"github.com/target/goalert/schedule"
	"github.com/target/goalert/schedule/rotation"
	"github.com/target/goalert/service"
	"github.com/target/goalert/user"
	"github.com/target/goalert/user/contactmethod"
	"github.com/target/goalert/user/notificationrule"

	"github.com/pkg/errors"
)

type dataLoaderKey int

const requestLoadersKey = dataLoaderKey(1)

// loaders contains all the dataloader instances for a single request.
// These are used to batch database queries and prevent N+1 problems in GraphQL resolvers.
type loaders struct {
	Alert                     *dataloader.Loader[int, alert.Alert]
	AlertState                *dataloader.Loader[int, alert.State]
	EP                        *dataloader.Loader[string, escalation.Policy]
	Rotation                  *dataloader.Loader[string, rotation.Rotation]
	Schedule                  *dataloader.Loader[string, schedule.Schedule]
	Service                   *dataloader.Loader[string, service.Service]
	User                      *dataloader.Loader[string, user.User]
	CM                        *dataloader.Loader[string, contactmethod.ContactMethod]
	NotificationMessageStatus *dataloader.Loader[string, notification.SendResult]
	NC                        *dataloader.Loader[string, notificationchannel.Channel]
	AlertFeedback             *dataloader.Loader[int, alert.Feedback]
	AlertMetadata             *dataloader.Loader[int, alert.MetadataAlertID]
	AlertsByStatus            *dataloader.AggFetcher[uuid.UUID, gadb.ServiceAlertCountsRow]
	CMsByUser                 *dataloader.AggFetcher[string, contactmethod.ContactMethod]
	NRsByUser                 *dataloader.AggFetcher[string, notificationrule.NotificationRule]
}

// registerLoaders creates and registers all dataloaders for the request context.
// Each loader is configured with appropriate batch settings and ID extraction functions.
func (a *App) registerLoaders(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, requestLoadersKey, &loaders{
		Alert:      dataloader.NewStoreLoader(ctx, a.AlertStore.FindMany, func(a alert.Alert) int { return a.ID }),
		AlertState: dataloader.NewStoreLoader(ctx, a.AlertStore.State, func(s alert.State) int { return s.ID }),
		EP:         dataloader.NewStoreLoader(ctx, a.PolicyStore.FindManyPolicies, func(p escalation.Policy) string { return p.ID }),
		Rotation:   dataloader.NewStoreLoader(ctx, a.RotationStore.FindMany, func(r rotation.Rotation) string { return r.ID }),
		Schedule:   dataloader.NewStoreLoader(ctx, a.ScheduleStore.FindMany, func(s schedule.Schedule) string { return s.ID }),
		Service:    dataloader.NewStoreLoader(ctx, a.ServiceStore.FindMany, func(s service.Service) string { return s.ID }),
		User:       dataloader.NewStoreLoader(ctx, a.UserStore.FindMany, func(u user.User) string { return u.ID }),
		CM: dataloader.NewStoreLoader(ctx, func(ctx context.Context, ids []string) ([]contactmethod.ContactMethod, error) {
			return a.CMStore.FindMany(ctx, nil, ids)
		}, func(cm contactmethod.ContactMethod) string { return cm.ID.String() }),
		NotificationMessageStatus: dataloader.NewStoreLoader(ctx, a.NotificationStore.FindManyMessageStatuses, func(n notification.SendResult) string { return n.ID }),
		NC:                        dataloader.NewStoreLoader(ctx, a.NCStore.FindMany, func(nc notificationchannel.Channel) string { return nc.ID.String() }),
		AlertFeedback:             dataloader.NewStoreLoader(ctx, a.AlertStore.Feedback, func(f alert.Feedback) int { return f.ID }),
		AlertMetadata: dataloader.NewStoreLoader(ctx, func(ctx context.Context, i []int) ([]alert.MetadataAlertID, error) {
			return a.AlertStore.FindManyMetadata(ctx, nil, i)
		}, func(md alert.MetadataAlertID) int { return int(md.ID) }),
		AlertsByStatus: dataloader.NewStoreLoaderAgg(ctx, a.AlertStore.ServiceAlertCounts, func(r gadb.ServiceAlertCountsRow) uuid.UUID { return r.ServiceID.UUID }),

		CMsByUser: dataloader.NewStoreLoaderAgg(ctx, func(ctx context.Context, userIDs []string) ([]contactmethod.ContactMethod, error) {
			return a.CMStore.FindAllByUserIDs(ctx, nil, userIDs)
		}, func(cm contactmethod.ContactMethod) string { return cm.UserID }),

		NRsByUser: dataloader.NewStoreLoaderAgg(ctx, a.NRStore.FindAllByUserIDs, func(nr notificationrule.NotificationRule) string { return nr.UserID }),
	})
	return ctx
}

// loadersFrom extracts the loaders struct from the request context.
// Returns an empty loaders struct if none are found.
func loadersFrom(ctx context.Context) loaders {
	loader, ok := ctx.Value(requestLoadersKey).(*loaders)
	if !ok {
		return loaders{}
	}
	if loader == nil {
		return loaders{}
	}

	return *loader
}

// closeLoaders closes all dataloaders in the request context to prevent goroutine leaks.
// This should be called when the request is complete.
func (a *App) closeLoaders(ctx context.Context) {
	loader := loadersFrom(ctx)

	if loader.Alert != nil {
		loader.Alert.Close()
	}
	if loader.AlertState != nil {
		loader.AlertState.Close()
	}
	if loader.EP != nil {
		loader.EP.Close()
	}
	if loader.Rotation != nil {
		loader.Rotation.Close()
	}
	if loader.Schedule != nil {
		loader.Schedule.Close()
	}
	if loader.Service != nil {
		loader.Service.Close()
	}
	if loader.User != nil {
		loader.User.Close()
	}
	if loader.CM != nil {
		loader.CM.Close()
	}

	if loader.NotificationMessageStatus != nil {
		loader.NotificationMessageStatus.Close()
	}
	if loader.NC != nil {
		loader.NC.Close()
	}

	if loader.AlertFeedback != nil {
		loader.AlertFeedback.Close()
	}
	if loader.AlertMetadata != nil {
		loader.AlertMetadata.Close()
	}
	if loader.AlertsByStatus != nil {
		loader.AlertsByStatus.Close()
	}

	if loader.CMsByUser != nil {
		loader.CMsByUser.Close()
	}
	if loader.NRsByUser != nil {
		loader.NRsByUser.Close()
	}
}

func (app *App) FindAlertCountByStatus(ctx context.Context, serviceID uuid.UUID) ([]gadb.ServiceAlertCountsRow, error) {
	loader := loadersFrom(ctx).AlertsByStatus
	if loader == nil {
		return app.AlertStore.ServiceAlertCounts(ctx, []uuid.UUID{serviceID})
	}

	return loader.FetchAggregate(ctx, serviceID)
}

func (app *App) FindOneAlertMetadata(ctx context.Context, id int) (map[string]string, error) {
	loader := loadersFrom(ctx).AlertMetadata
	if loader == nil {
		return app.AlertStore.Metadata(ctx, nil, id)
	}

	md, err := loader.FetchOne(ctx, id)
	if err != nil {
		return nil, err
	}
	if md == nil {
		return map[string]string{}, nil
	}

	return md.Meta, nil
}

func (app *App) FindOneNotificationMessageStatus(ctx context.Context, id string) (*notification.SendResult, error) {
	loader := loadersFrom(ctx).NotificationMessageStatus
	if loader == nil {
		ms, err := app.NotificationStore.FindManyMessageStatuses(ctx, []string{id})
		if err != nil {
			return nil, err
		}
		return &ms[0], nil
	}

	return loader.FetchOne(ctx, id)
}

func (app *App) FindOneAlertFeedback(ctx context.Context, id int) (*alert.Feedback, error) {
	loader := loadersFrom(ctx).AlertFeedback
	if loader == nil {
		feedback, err := app.AlertStore.Feedback(ctx, []int{id})
		if err != nil {
			return nil, err
		}
		if len(feedback) == 0 {
			return nil, nil
		}
		return &feedback[0], nil
	}

	return loader.FetchOne(ctx, id)
}

func (app *App) FindOneRotation(ctx context.Context, id string) (*rotation.Rotation, error) {
	loader := loadersFrom(ctx).Rotation
	if loader == nil {
		return app.RotationStore.FindRotation(ctx, id)
	}

	return loader.FetchOne(ctx, id)
}

func (app *App) FindOneSchedule(ctx context.Context, id string) (*schedule.Schedule, error) {
	loader := loadersFrom(ctx).Schedule
	if loader == nil {
		return app.ScheduleStore.FindOne(ctx, id)
	}

	return loader.FetchOne(ctx, id)
}

func (app *App) FindOneUser(ctx context.Context, id string) (*user.User, error) {
	loader := loadersFrom(ctx).User
	if loader == nil {
		return app.UserStore.FindOne(ctx, id)
	}

	return loader.FetchOne(ctx, id)
}

// FindOneCM will return a single contact method for the given id, using the contexts dataloader if enabled.
func (app *App) FindOneCM(ctx context.Context, id uuid.UUID) (cm *contactmethod.ContactMethod, err error) {
	loader := loadersFrom(ctx).CM
	if loader == nil {
		cm, err = app.CMStore.FindOne(ctx, nil, id)
	} else {
		cm, err = loader.FetchOne(ctx, id.String())
	}
	if err != nil {
		return nil, err
	}
	if cm == nil {
		return nil, nil
	}
	if cm.Private && cm.UserID != permission.UserID(ctx) {
		return nil, nil
	}

	return cm, nil
}

// FindManyCMsForUser will return all contact methods for the given user ID, using the context's
// dataloader if enabled, filtering out private contact methods not owned by the requester.
func (app *App) FindManyCMsForUser(ctx context.Context, userID string) ([]contactmethod.ContactMethod, error) {
	loader := loadersFrom(ctx).CMsByUser
	var cms []contactmethod.ContactMethod
	var err error
	if loader == nil {
		cms, err = app.CMStore.FindAllByUserIDs(ctx, nil, []string{userID})
	} else {
		cms, err = loader.FetchAggregate(ctx, userID)
	}
	if err != nil {
		return nil, err
	}

	return filterVisibleContactMethods(ctx, cms), nil
}

// filterVisibleContactMethods removes private contact methods not owned by the requesting
// user, matching the same rule FindOneCM applies to a single contact method.
func filterVisibleContactMethods(ctx context.Context, cms []contactmethod.ContactMethod) []contactmethod.ContactMethod {
	authID := permission.UserID(ctx)
	result := make([]contactmethod.ContactMethod, 0, len(cms))
	for _, cm := range cms {
		if cm.Private && cm.UserID != authID {
			continue
		}
		result = append(result, cm)
	}
	return result
}

// FindManyNRsForUser will return all notification rules for the given user ID, using the
// context's dataloader if enabled.
func (app *App) FindManyNRsForUser(ctx context.Context, userID string) ([]notificationrule.NotificationRule, error) {
	loader := loadersFrom(ctx).NRsByUser
	if loader == nil {
		return app.NRStore.FindAllByUserIDs(ctx, []string{userID})
	}

	return loader.FetchAggregate(ctx, userID)
}

// FindOneNC will return a single notification channel for the given id, using the contexts dataloader if enabled.
func (app *App) FindOneNC(ctx context.Context, id uuid.UUID) (*notificationchannel.Channel, error) {
	loader := loadersFrom(ctx).NC
	if loader == nil {
		return app.NCStore.FindOne(ctx, id)
	}

	return loader.FetchOne(ctx, id.String())
}

func (app *App) FindOnePolicy(ctx context.Context, id string) (*escalation.Policy, error) {
	loader := loadersFrom(ctx).EP
	if loader == nil {
		return app.PolicyStore.FindOnePolicyTx(ctx, nil, id)
	}

	return loader.FetchOne(ctx, id)
}

func (app *App) FindOneService(ctx context.Context, id string) (*service.Service, error) {
	loader := loadersFrom(ctx).Service
	if loader == nil {
		return app.ServiceStore.FindOne(ctx, id)
	}

	return loader.FetchOne(ctx, id)
}

func (app *App) FindOneAlertState(ctx context.Context, alertID int) (*alert.State, error) {
	loader := loadersFrom(ctx).AlertState
	if loader == nil {
		epState, err := app.AlertStore.State(ctx, []int{alertID})
		if err != nil {
			return nil, err
		}
		if len(epState) == 0 {
			return nil, errors.New("no current epState for alert")
		}
		return &epState[0], nil
	}

	return loader.FetchOne(ctx, alertID)
}

func (app *App) FindOneAlert(ctx context.Context, id int) (*alert.Alert, error) {
	loader := loadersFrom(ctx).Alert
	if loader == nil {
		return app.AlertStore.FindOne(ctx, id)
	}

	return loader.FetchOne(ctx, id)
}
