setInterval(searchData, 50000);


function searchData(event) {

    event.preventDefault();
    console.log("searchDataRunning")
    const searchInput = document.getElementById("search-msg").value;
    if (searchInput.length == 0) {
        document.getElementById('search-msg').innerHTML = "Please Enter A Keyword";
    }
    else {
        sdk.dashboardGet(
            { 'q': searchInput },
            {},
            {}
        )
            .then((response) => {
                if (response.data) {
                    console.log(response.data)



                    getcurrent_emoji(response.data)
                    gethistory_emoji(response.data)

                    getcurrent_language(response.data)
                    gethistory_language(response.data)

                    getcurrent_comment(response.data)
                    gethistory_comment(response.data)

                    getcurrent_sentiment(response.data)
                    gethistory_sentiment(response.data)

                    // getcurrent_words(response.data)

                    getcurrent_words(response.data)
                    gethistory_words(response.data)
                }
            })
            .catch((error) => {
                console.log("error", error);
            });
    }

}


function getcurrent_words(response) {
    const data = response.current_words.Content.map(i => ({
        name: i.words,
        value: parseInt(i.count),
    }));

    // console.log(data)

    echarts.init(document.querySelector("#current_wordcloud")).setOption({

        tooltip: {},
        series: [{
            type: 'wordCloud',
            gridSize: 2,
            sizeRange: [12, 50],
            rotationRange: [-90, 90],
            shape: 'circle',
            width: 600,
            height: 400,
            drawOutOfBound: true,
            textStyle: {
                color: function () {
                    return 'rgb(' + [
                        Math.round(Math.random() * 160),
                        Math.round(Math.random() * 160),
                        Math.round(Math.random() * 160)
                    ].join(',') + ')';
                }
            },
            emphasis: {
                textStyle: {
                    shadowBlur: 10,
                    shadowColor: '#333'
                }
            },

            data: data
        }]
    });
}

function gethistory_words(response) {
    const data = response.history_words.Content.map(i => ({
        name: i.words,
        value: parseInt(i.count),
    }));

    //.log(data)

    echarts.init(document.querySelector("#history_wordcloud")).setOption({

        tooltip: {},
        series: [{
            type: 'wordCloud',
            gridSize: 2,
            sizeRange: [12, 50],
            rotationRange: [-90, 90],
            shape: 'circle',
            width: 600,
            height: 400,
            drawOutOfBound: true,
            textStyle: {
                color: function () {
                    return 'rgb(' + [
                        Math.round(Math.random() * 160),
                        Math.round(Math.random() * 160),
                        Math.round(Math.random() * 160)
                    ].join(',') + ')';
                }
            },
            emphasis: {
                textStyle: {
                    shadowBlur: 10,
                    shadowColor: '#333'
                }
            },

            data: data
        }]
    });
}

function getcurrent_emoji(response) {

    echarts.init(document.querySelector("#current_emoji")).setOption({
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'shadow'
            }
        },
        legend: {},
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'value',
            boundaryGap: [0, 0.01]
        },
        yAxis: {
            type: 'category',
            data: response.current_emoji.Content.map(i => i.emoji)
        },
        series: [{
            name: 'current emoji',
            type: 'bar',
            data: response.current_emoji.Content.map(i => { return parseInt(i.count) })
        }
        ]
    });
}

function gethistory_emoji(response) {

    echarts.init(document.querySelector("#history_emoji")).setOption({
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'shadow'
            }
        },
        legend: {},
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'value',
            boundaryGap: [0, 0.01]
        },
        yAxis: {
            type: 'category',
            data: response.history_emoji.Content.map(i => i.emoji)
        },
        series: [{
            name: 'history emoji',
            type: 'bar',
            data: response.history_emoji.Content.map(i => { return parseInt(i.count) })
        }
        ]
    });
}


function getcurrent_language(response) {

    echarts.init(document.querySelector("#current_language")).setOption({
        xAxis: {
            type: 'category',
            data: response.current_language.Content.map(i => i.Bytes)
        },
        yAxis: {
            type: 'value'
        },
        series: [{
            type: 'bar',
            data: response.current_language.Content.map(i => { return parseInt(i.count) })
        }]
    });
}

function gethistory_language(response) {
    //console.log("gethistory_language runnning")

    echarts.init(document.querySelector("#history_language")).setOption({
        xAxis: {
            type: 'category',
            data: response.history_language.Content.map(i => i.Bytes)
        },
        yAxis: {
            type: 'value'
        },
        series: [{
            type: 'bar',
            data: response.history_language.Content.map(i => { return parseInt(i.count) })
        }]
    });
}

function getcurrent_comment(response) {

    console.log("getcurrent_comment");

    var comment = document.getElementById("current_comment");

    var data = 0;
    var resource = response.current_sentiment.Content;
    for (var i = 0; i < resource.length; i++) {
        data = data + parseInt(resource[i].count);
    }
    console.log(data)

    comment.innerHTML = data;
}


function gethistory_comment(response) {
    echarts.init(document.querySelector("#history_comment")).setOption({
        tooltip: {
            trigger: 'axis'
        },
        legend: {
            data: ['comment']
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'category',
            boundaryGap: false,
            data: response.history_comments.Content.map(i => i.window_start)
        },
        yAxis: {
            type: 'value'
        },
        series: [
            {
                name: 'comment',
                type: 'line',
                stack: 'Total',
                data: response.history_comments.Content.map(i => { return parseInt(i.count) })
            }
        ]
    });
};


function getcurrent_sentiment(response) {

    const data = response.current_sentiment.Content.map(i => ({
        name: i.sentiment_class,
        value: parseInt(i.count),
    }));
    echarts.init(document.querySelector("#current_sentiment")).setOption({
        title: {
            text: '',
            subtext: '',
            left: 'center'
        },
        tooltip: {
            trigger: 'item'
        },
        legend: {
            orient: 'vertical',
            left: 'left'
        },
        series: [{
            name: 'Access From',
            type: 'pie',
            radius: '50%',
            data: data,
            emphasis: {
                itemStyle: {
                    shadowBlur: 10,
                    shadowOffsetX: 0,
                    shadowColor: 'rgba(0, 0, 0, 0.5)'
                }
            }
        }]
    });
}

function gethistory_sentiment(response) {
    var time = []
    var resource = response.history_negative.Content;
    time.push('sentiment');
    for (var i = 0; i < resource.length; i++) {
        time.push(resource[i].window_start)
    }

    var negative = []
    var resource_negative = response.history_negative.Content;
    negative.push('negative');
    for (var i = 0; i < resource_negative.length; i++) {
        negative.push(resource_negative[i].count)
    }

    var neutral = []
    var resource_neutral = response.history_neutral.Content;
    neutral.push('neutral');
    for (var i = 0; i < resource_neutral.length; i++) {
        neutral.push(resource_neutral[i].count)
    }

    var positive = []
    var resource_positive = response.history_positive.Content;
    positive.push('positive');
    for (var i = 0; i < resource_positive.length; i++) {
        positive.push(resource_positive[i].count)
    }

    //console.log(negative)
    var myChart = echarts.init(document.querySelector("#history_sentiment"))
    option = {
        legend: {},
        tooltip: {
            trigger: 'axis',
            showContent: false
        },
        dataset: {
            source: [
                time,
                neutral,
                positive,
                negative
            ]
        },
        xAxis: { type: 'category' },
        yAxis: { gridIndex: 0 },
        grid: { top: '55%' },
        series: [
            {
                type: 'line',
                smooth: true,
                seriesLayoutBy: 'row',
                emphasis: { focus: 'series' }
            },
            {
                type: 'line',
                smooth: true,
                seriesLayoutBy: 'row',
                emphasis: { focus: 'series' }
            },
            {
                type: 'line',
                smooth: true,
                seriesLayoutBy: 'row',
                emphasis: { focus: 'series' }
            },
            {
                type: 'pie',
                id: 'pie',
                radius: '30%',
                center: ['50%', '25%'],
                emphasis: {
                    focus: 'self'
                },
                label: {
                    formatter: '{b}: {@2012} ({d}%)'
                },
                encode: {
                    itemName: 'sentiment',
                    value: '2012',
                    tooltip: '2012'
                }
            }
        ]
    };
    myChart.on('updateAxisPointer', function (event) {
        const xAxisInfo = event.axesInfo[0];
        if (xAxisInfo) {
            const dimension = xAxisInfo.value + 1;
            myChart.setOption({
                series: {
                    id: 'pie',
                    label: {
                        formatter: '{b}: {@[' + dimension + ']} ({d}%)'
                    },
                    encode: {
                        value: dimension,
                        tooltip: dimension
                    }
                }
            });
        }
    });
    myChart.setOption(option);


}


(function () {
    "use strict";

    /**
     * Easy selector helper function
     */
    const select = (el, all = false) => {
        el = el.trim()
        if (all) {
            return [...document.querySelectorAll(el)]
        } else {
            return document.querySelector(el)
        }



    }

    /**
     * Easy event listener 
     * 
     
     */
    const on = (type, el, listener, all = false) => {
        if (all) {
            select(el, all).forEach(e => e.addEventListener(type, listener))
        } else {
            select(el, all).addEventListener(type, listener)
        }
    }

    /**
     * Easy on scroll event listener 
     */
    const onscroll = (el, listener) => {
        el.addEventListener('scroll', listener)
    }

    /**
     * Sidebar toggle
     */
    if (select('.toggle-sidebar-btn')) {
        on('click', '.toggle-sidebar-btn', function (e) {
            select('body').classList.toggle('toggle-sidebar')
        })
    }

    /**
     * Search bar toggle
     */
    if (select('.search-bar-toggle')) {
        on('click', '.search-bar-toggle', function (e) {
            select('.search-bar').classList.toggle('search-bar-show')
        })
    }

    /**
     * Navbar links active state on scroll
     */
    let navbarlinks = select('#navbar .scrollto', true)
    const navbarlinksActive = () => {
        let position = window.scrollY + 200
        navbarlinks.forEach(navbarlink => {
            if (!navbarlink.hash) return
            let section = select(navbarlink.hash)
            if (!section) return
            if (position >= section.offsetTop && position <= (section.offsetTop + section.offsetHeight)) {
                navbarlink.classList.add('active')
            } else {
                navbarlink.classList.remove('active')
            }
        })
    }
    window.addEventListener('load', navbarlinksActive)
    onscroll(document, navbarlinksActive)

    /**
     * Toggle .header-scrolled class to #header when page is scrolled
     */
    let selectHeader = select('#header')
    if (selectHeader) {
        const headerScrolled = () => {
            if (window.scrollY > 100) {
                selectHeader.classList.add('header-scrolled')
            } else {
                selectHeader.classList.remove('header-scrolled')
            }
        }
        window.addEventListener('load', headerScrolled)
        onscroll(document, headerScrolled)
    }

    /**
     * Back to top button
     */
    let backtotop = select('.back-to-top')
    if (backtotop) {
        const toggleBacktotop = () => {
            if (window.scrollY > 100) {
                backtotop.classList.add('active')
            } else {
                backtotop.classList.remove('active')
            }
        }
        window.addEventListener('load', toggleBacktotop)
        onscroll(document, toggleBacktotop)
    }

    /**
     * Initiate tooltips
     */
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'))
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl)
    })

    /**
     * Initiate quill editors
     */
    if (select('.quill-editor-default')) {
        new Quill('.quill-editor-default', {
            theme: 'snow'
        });
    }

    if (select('.quill-editor-bubble')) {
        new Quill('.quill-editor-bubble', {
            theme: 'bubble'
        });
    }

    if (select('.quill-editor-full')) {
        new Quill(".quill-editor-full", {
            modules: {
                toolbar: [
                    [{
                        font: []
                    }, {
                        size: []
                    }],
                    ["bold", "italic", "underline", "strike"],
                    [{
                        color: []
                    },
                    {
                        background: []
                    }
                    ],
                    [{
                        script: "super"
                    },
                    {
                        script: "sub"
                    }
                    ],
                    [{
                        list: "ordered"
                    },
                    {
                        list: "bullet"
                    },
                    {
                        indent: "-1"
                    },
                    {
                        indent: "+1"
                    }
                    ],
                    ["direction", {
                        align: []
                    }],
                    ["link", "image", "video"],
                    ["clean"]
                ]
            },
            theme: "snow"
        });
    }

    /**
     * Initiate TinyMCE Editor
     */
    const useDarkMode = window.matchMedia('(prefers-color-scheme: dark)').matches;
    const isSmallScreen = window.matchMedia('(max-width: 1023.5px)').matches;

    tinymce.init({
        selector: 'textarea.tinymce-editor',
        plugins: 'preview importcss searchreplace autolink autosave save directionality code visualblocks visualchars fullscreen image link media template codesample table charmap pagebreak nonbreaking anchor insertdatetime advlist lists wordcount help charmap quickbars emoticons',
        editimage_cors_hosts: ['picsum.photos'],
        menubar: 'file edit view insert format tools table help',
        toolbar: 'undo redo | bold italic underline strikethrough | fontfamily fontsize blocks | alignleft aligncenter alignright alignjustify | outdent indent |  numlist bullist | forecolor backcolor removeformat | pagebreak | charmap emoticons | fullscreen  preview save print | insertfile image media template link anchor codesample | ltr rtl',
        toolbar_sticky: true,
        toolbar_sticky_offset: isSmallScreen ? 102 : 108,
        autosave_ask_before_unload: true,
        autosave_interval: '30s',
        autosave_prefix: '{path}{query}-{id}-',
        autosave_restore_when_empty: false,
        autosave_retention: '2m',
        image_advtab: true,
        link_list: [{
            title: 'My page 1',
            value: 'https://www.tiny.cloud'
        },
        {
            title: 'My page 2',
            value: 'http://www.moxiecode.com'
        }
        ],
        image_list: [{
            title: 'My page 1',
            value: 'https://www.tiny.cloud'
        },
        {
            title: 'My page 2',
            value: 'http://www.moxiecode.com'
        }
        ],
        image_class_list: [{
            title: 'None',
            value: ''
        },
        {
            title: 'Some class',
            value: 'class-name'
        }
        ],
        importcss_append: true,
        file_picker_callback: (callback, value, meta) => {
            /* Provide file and text for the link dialog */
            if (meta.filetype === 'file') {
                callback('https://www.google.com/logos/google.jpg', {
                    text: 'My text'
                });
            }

            /* Provide image and alt text for the image dialog */
            if (meta.filetype === 'image') {
                callback('https://www.google.com/logos/google.jpg', {
                    alt: 'My alt text'
                });
            }

            /* Provide alternative source and posted for the media dialog */
            if (meta.filetype === 'media') {
                callback('movie.mp4', {
                    source2: 'alt.ogg',
                    poster: 'https://www.google.com/logos/google.jpg'
                });
            }
        },
        templates: [{
            title: 'New Table',
            description: 'creates a new table',
            content: '<div class="mceTmpl"><table width="98%%"  border="0" cellspacing="0" cellpadding="0"><tr><th scope="col"> </th><th scope="col"> </th></tr><tr><td> </td><td> </td></tr></table></div>'
        },
        {
            title: 'Starting my story',
            description: 'A cure for writers block',
            content: 'Once upon a time...'
        },
        {
            title: 'New list with dates',
            description: 'New List with dates',
            content: '<div class="mceTmpl"><span class="cdate">cdate</span><br><span class="mdate">mdate</span><h2>My List</h2><ul><li></li><li></li></ul></div>'
        }
        ],
        template_cdate_format: '[Date Created (CDATE): %m/%d/%Y : %H:%M:%S]',
        template_mdate_format: '[Date Modified (MDATE): %m/%d/%Y : %H:%M:%S]',
        height: 600,
        image_caption: true,
        quickbars_selection_toolbar: 'bold italic | quicklink h2 h3 blockquote quickimage quicktable',
        noneditable_class: 'mceNonEditable',
        toolbar_mode: 'sliding',
        contextmenu: 'link image table',
        skin: useDarkMode ? 'oxide-dark' : 'oxide',
        content_css: useDarkMode ? 'dark' : 'default',
        content_style: 'body { font-family:Helvetica,Arial,sans-serif; font-size:16px }'
    });

    /**
     * Initiate Bootstrap validation check
     */
    var needsValidation = document.querySelectorAll('.needs-validation')

    Array.prototype.slice.call(needsValidation)
        .forEach(function (form) {
            form.addEventListener('submit', function (event) {
                if (!form.checkValidity()) {
                    event.preventDefault()
                    event.stopPropagation()
                }

                form.classList.add('was-validated')
            }, false)
        })

    /**
     * Initiate Datatables
     */
    const datatables = select('.datatable', true)
    datatables.forEach(datatable => {
        new simpleDatatables.DataTable(datatable);
    })

    /**
     * Autoresize echart charts
     */
    const mainContainer = select('#main');
    if (mainContainer) {
        setTimeout(() => {
            new ResizeObserver(function () {
                select('.echart', true).forEach(getEchart => {
                    echarts.getInstanceByDom(getEchart).resize();
                })
            }).observe(mainContainer);
        }, 200);
    }

})();
