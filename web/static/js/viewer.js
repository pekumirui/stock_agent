/**
 * 決算分析リアルタイムビューア - Alpine.jsアプリケーション
 */
function viewerApp() {
    return {
        targetDate: document.getElementById('date-picker')?.value || new Date().toISOString().slice(0, 10),
        filters: {
            earnings: true,
            revision: true,
            dividend: true,
            other: true,
        },
        sort: 'time',
        order: 'desc',

        get rowCount() {
            return document.querySelectorAll('#table-body tr.data-row').length;
        },

        get activeTypes() {
            const types = [];
            if (this.filters.earnings) types.push('earnings');
            if (this.filters.revision) types.push('revision');
            if (this.filters.dividend) types.push('dividend');
            if (this.filters.other) types.push('other');
            return types;
        },

        refreshTable() {
            const params = new URLSearchParams();
            params.set('date', this.targetDate);
            if (this.activeTypes.length < 4) {
                params.set('types', this.activeTypes.join(','));
            }
            params.set('sort', this.sort);
            params.set('order', this.order);

            htmx.ajax('GET', '/viewer/table?' + params.toString(), {
                target: '#table-body',
                swap: 'innerHTML',
            });
        },

        expandAll() {
            document.querySelectorAll('.detail-row').forEach(row => {
                row.style.display = 'table-row';
                row.classList.add('visible');
            });
            document.querySelectorAll('.toggle-btn').forEach(btn => {
                btn.innerHTML = '&#9660;';
                btn.classList.add('expanded');
                // 未ロードの展開行はhtmxでロード
                const detailRow = btn.closest('tr').nextElementSibling;
                if (detailRow && detailRow.classList.contains('detail-row')) {
                    const td = detailRow.querySelector('td');
                    if (td && td.innerHTML.trim() === '') {
                        htmx.trigger(btn, 'click');
                    }
                }
            });
        },

        collapseAll() {
            document.querySelectorAll('.detail-row').forEach(row => {
                row.style.display = 'none';
                row.classList.remove('visible');
            });
            document.querySelectorAll('.toggle-btn').forEach(btn => {
                btn.innerHTML = '&#9654;';
                btn.classList.remove('expanded');
            });
        },
    };
}

/**
 * 展開行のトグル
 */
function toggleDetail(btn) {
    const dataRow = btn.closest('tr');
    const detailRow = dataRow.nextElementSibling;
    if (!detailRow || !detailRow.classList.contains('detail-row')) return;

    const isVisible = detailRow.style.display === 'table-row';

    if (isVisible) {
        detailRow.style.display = 'none';
        detailRow.classList.remove('visible');
        btn.innerHTML = '&#9654;';
        btn.classList.remove('expanded');
    } else {
        detailRow.style.display = 'table-row';
        detailRow.classList.add('visible');
        btn.innerHTML = '&#9660;';
        btn.classList.add('expanded');
    }
}

/* htmx afterSwap イベントで件数を更新 */
document.addEventListener('htmx:afterSwap', function(event) {
    if (event.detail.target.id === 'table-body') {
        /* Alpine.jsのrowCountを自動更新させるためにイベントを発行 */
        window.dispatchEvent(new Event('resize'));
    }
});
