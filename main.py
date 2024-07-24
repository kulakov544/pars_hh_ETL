from prefect import serve
from prefect.server.schemas.schedules import CronSchedule

from flow_pars_hh_dir.flow_pars_hh import flow_pars_hh
from flow_pars_hh_dir.flow_get_rates_utilit import flow_get_and_put_rates

if __name__ == "__main__":
    # Подготовка к деплою
    pars_hh = flow_pars_hh.to_deployment(name='flow_pars_hh_dir',
                                         #work_pool_name="default-agent-pool",
                                         schedule=(CronSchedule(cron="0 3 * * *", timezone="Europe/Moscow")),
                                         tags=["parser"],
                                         description="Парсинг данных с hh",
                                         version="1.0", )
    get_rates = flow_get_and_put_rates.to_deployment(name='flow_get_rates',
                                         # work_pool_name="default-agent-pool",
                                         schedule=(CronSchedule(cron="0 0-5 * * *", timezone="Europe/Moscow")),
                                         tags=["parser"],
                                         description="Получение курсов валют",
                                         version="1.0", )

    serve(pars_hh, get_rates)
