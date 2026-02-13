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
        selectedTicker: null,  // Currently selected ticker for detail panel

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

        selectRow(ticker) {
            if (this.selectedTicker === ticker) {
                // Same row clicked - deselect
                this.selectedTicker = null;
                document.getElementById('financial-detail-target').innerHTML =
                    '<p class="detail-empty" style="padding: 40px 16px; text-align: center;">テーブルの銘柄をクリックして業績詳細を表示</p>';
                return;
            }

            this.selectedTicker = ticker;

            // Load financial detail via htmx
            htmx.ajax('GET', '/viewer/financial-detail/' + ticker, {
                target: '#financial-detail-target',
                swap: 'innerHTML',
            });
        },
    };
}

/* htmx afterSwap - update row count and re-apply selected state */
document.addEventListener('htmx:afterSwap', function(event) {
    if (event.detail.target.id === 'table-body') {
        window.dispatchEvent(new Event('resize'));
    }
});
