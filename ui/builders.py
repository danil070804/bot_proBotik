from html import escape

from telebot import types


def _value_text(value, style='plain'):
    raw = '—' if value in [None, ''] else str(value)
    safe = escape(raw)
    if style == 'code':
        return f'<code>{safe}</code>'
    if style == 'bold':
        return f'<b>{safe}</b>'
    return safe


def build_section_screen(title, stats=None, description=None, path=None, footer=None):
    lines = []
    if path:
        lines.append(escape(str(path)))
        lines.append('')
    lines.append(f'<b>{escape(str(title))}</b>')
    if stats:
        lines.append('')
        for item in stats:
            label = escape(str(item[0]))
            value = _value_text(item[1], style='bold')
            suffix = escape(str(item[2])) if len(item) > 2 and item[2] else ''
            lines.append(f'{label}: {value}{suffix}')
    if description:
        lines.append('')
        lines.append(escape(str(description)))
    if footer:
        lines.append('')
        lines.append(escape(str(footer)))
    return '\n'.join(lines)


def build_list_page(title, page, total_pages, items, path=None):
    lines = []
    if path:
        lines.append(escape(str(path)))
        lines.append('')
    lines.append(f'<b>{escape(str(title))} · {int(page) + 1}/{int(total_pages)}</b>')
    lines.append('')
    for idx, item in enumerate(items, start=1):
        badge = f'{item.get("badge")} ' if item.get('badge') else ''
        primary = escape(str(item.get('primary') or '—'))
        lines.append(f'{idx}. {badge}{primary}'.rstrip())
        secondary = item.get('secondary')
        if secondary:
            lines.append(escape(str(secondary)))
        meta = item.get('meta')
        if meta:
            lines.append(escape(str(meta)))
        lines.append('')
    return '\n'.join(lines).strip()


def build_entity_card(title, fields, path=None, footer=None):
    lines = []
    if path:
        lines.append(escape(str(path)))
        lines.append('')
    lines.append(f'<b>{escape(str(title))}</b>')
    lines.append('')
    for field in fields:
        label = escape(str(field[0]))
        value = field[1]
        style = field[2] if len(field) > 2 else 'plain'
        lines.append(f'{label}: {_value_text(value, style=style)}')
    if footer:
        lines.append('')
        lines.append(escape(str(footer)))
    return '\n'.join(lines)


def build_status_screen(title, stats=None, highlights=None, path=None, footer=None):
    lines = []
    if path:
        lines.append(escape(str(path)))
        lines.append('')
    lines.append(f'<b>{escape(str(title))}</b>')
    if stats:
        lines.append('')
        for field in stats:
            label = escape(str(field[0]))
            value = _value_text(field[1], style='bold')
            suffix = escape(str(field[2])) if len(field) > 2 and field[2] else ''
            lines.append(f'{label}: {value}{suffix}')
    if highlights:
        lines.append('')
        for item in highlights:
            lines.append(f'• {escape(str(item))}')
    if footer:
        lines.append('')
        lines.append(escape(str(footer)))
    return '\n'.join(lines)


def build_confirm_screen(title, summary=None, confirm_label='Подтвердить', cancel_label='Отмена', path=None):
    lines = []
    if path:
        lines.append(escape(str(path)))
        lines.append('')
    lines.append(f'<b>{escape(str(title))}</b>')
    if summary:
        lines.append('')
        if isinstance(summary, (list, tuple)):
            for item in summary:
                lines.append(f'• {escape(str(item))}')
        else:
            lines.append(escape(str(summary)))
    lines.append('')
    lines.append(f'Действия: {escape(str(confirm_label))} / {escape(str(cancel_label))}')
    return '\n'.join(lines)


def build_inline_keyboard(rows):
    keyboard = types.InlineKeyboardMarkup()
    for row in rows:
        buttons = []
        for spec in row:
            if not spec:
                continue
            text, callback_data = spec
            buttons.append(types.InlineKeyboardButton(text=text, callback_data=callback_data))
        if buttons:
            keyboard.row(*buttons)
    return keyboard
