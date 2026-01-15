# menu_spec.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, Literal

ItemKind = Literal["page", "command", "separator"]

@dataclass(frozen=True)
class MenuEntry:
    kind: ItemKind
    label: str
    # для kind="page"
    key: Optional[str] = None
    perm: Optional[str] = None
    # для каталога прав
    group: str = "core"
    title: Optional[str] = None

@dataclass(frozen=True)
class MenuSection:
    label: str                     # корневой пункт menubar
    entries: List[MenuEntry]

MENU_SPEC: List[MenuSection] = [
    MenuSection(
        label="Объектный табель",
        entries=[
            MenuEntry("page", "Создать", key="timesheet", perm="page.timesheet", group="timesheets", title="Объектный табель: Создать"),
            MenuEntry("page", "Мои табели", key="my_timesheets", perm="page.my_timesheets", group="timesheets", title="Объектный табель: Мои табели"),
            MenuEntry("page", "Реестр табелей", key="timesheet_registry", perm="page.timesheet_registry", group="timesheets", title="Объектный табель: Реестр табелей"),
            MenuEntry("page", "Работники", key="workers", perm="page.workers", group="timesheets", title="Объектный табель: Работники"),
            MenuEntry("page", "Сравнение с 1С", key="timesheet_compare", perm="page.timesheet_compare", group="timesheets", title="Объектный табель: Сравнение с 1С"),
        ],
    ),
    MenuSection(
        label="Автотранспорт",
        entries=[
            MenuEntry("page", "Создать заявку", key="transport", perm="page.transport", group="transport", title="Автотранспорт: Создать заявку"),
            MenuEntry("page", "Мои заявки", key="my_transport_orders", perm="page.my_transport_orders", group="transport", title="Автотранспорт: Мои заявки"),
            MenuEntry("page", "Планирование", key="planning", perm="page.planning", group="transport", title="Автотранспорт: Планирование"),
            MenuEntry("page", "Реестр", key="transport_registry", perm="page.transport_registry", group="transport", title="Автотранспорт: Реестр"),
        ],
    ),
    MenuSection(
        label="Питание",
        entries=[
            MenuEntry("page", "Создать заявку", key="meals_order", perm="page.meals_order", group="meals", title="Питание: Создать заявку"),
            MenuEntry("page", "Мои заявки", key="my_meals_orders", perm="page.my_meals_orders", group="meals", title="Питание: Мои заявки"),
            MenuEntry("page", "Планирование", key="meals_planning", perm="page.meals_planning", group="meals", title="Питание: Планирование"),
            MenuEntry("page", "Реестр", key="meals_registry", perm="page.meals_registry", group="meals", title="Питание: Реестр"),
            MenuEntry("page", "Работники (питание)", key="meals_workers", perm="page.meals_workers", group="meals", title="Питание: Работники"),
            MenuEntry("page", "Настройки", key="meals_settings", perm="page.meals_settings", group="meals", title="Питание: Настройки"),
        ],
    ),
    MenuSection(
        label="Проживание",
        entries=[
            MenuEntry("page", "Реестр проживаний", key="lodging_registry", perm="page.lodging_registry", group="lodging", title="Проживание: Реестр"),
            MenuEntry("page", "Общежития и комнаты", key="lodging_dorms", perm="page.lodging_dorms", group="lodging", title="Проживание: Общежития и комнаты"),
            MenuEntry("page", "Тарифы (цена за сутки)", key="lodging_rates", perm="page.lodging_rates", group="lodging", title="Проживание: Тарифы"),
        ],
    ),
    MenuSection(
        label="Объекты",
        entries=[
            MenuEntry("page", "Создать/Редактировать", key="object_create", perm="page.object_create", group="objects", title="Объекты: Создать/Редактировать"),
            MenuEntry("page", "Реестр", key="objects_registry", perm="page.objects_registry", group="objects", title="Объекты: Реестр"),
        ],
    ),
    MenuSection(
        label="Аналитика",
        entries=[
            MenuEntry("page", "Операционная аналитика", key="analytics_dashboard", perm="page.analytics_dashboard", group="analytics", title="Аналитика: Операционная"),
        ],
    ),
    MenuSection(
        label="Инструменты",
        entries=[
            MenuEntry("page", "Анализ смет", key="budget", perm="page.budget", group="tools", title="Инструменты: Анализ смет"),
        ],
    ),
]

# top-level permissions, не “страницы”
TOP_LEVEL = [
    MenuEntry("command", "Настройки", perm="page.settings", group="core", title="Настройки программы"),
]
