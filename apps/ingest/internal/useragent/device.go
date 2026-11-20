package useragent

import "openpanel/ingest/internal/jsregex"

var (
	samsungMobileRegex = jsregex.MustCompile(`SM-[ABDEFGJMNRWZ][0-9]+`, "i")
	samsungTabletRegex = jsregex.MustCompile(`SM-T[0-9]+`, "i")
	lgMobileRegex      = jsregex.MustCompile(`LG-[A-Z0-9]+`, "i")
	mobileRegex1       = jsregex.MustCompile(`(android|bb\d+|meego).+mobile|avantgo|bada\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od)|iris|kindle|lge |maemo|midp|mmp|mobile.+firefox|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\.(browser|link)|vodafone|wap|windows ce|xda|xiino`, "i")
	mobileRegex2       = jsregex.MustCompile(`1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw-(n|u)|c55\/|capi|ccwa|cdm-|cell|chtm|cldc|cmd-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc-s|devi|dica|dmob|do(c|p)o|ds(12|-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(-|_)|g1 u|g560|gene|gf-5|g-mo|go(\.w|od)|gr(ad|un)|haie|hcit|hd-(m|p|t)|hei-|hi(pt|ta)|hp( i|ip)|hs-c|ht(c(-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i-(20|go|ma)|i230|iac( |-|\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\/)|klon|kpt |kwc-|kyo(c|k)|le(no|xi)|lg( g|\/(k|l|u)|50|54|-[a-w])|libw|lynx|m1-w|m3ga|m50\/|ma(te|ui|xo)|mc(01|21|ca)|m-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|-([1-8]|c))|phil|pire|pl(ay|uc)|pn-2|po(ck|rt|se)|prox|psio|pt-g|qa-a|qc(07|12|21|32|60|-[2-7]|i-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h-|oo|p-)|sdk\/|se(c(-|0|1)|47|mc|nd|ri)|sgh-|shar|sie(-|m)|sk-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h-|v-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl-|tdg-|tel(i|m)|tim-|t-mo|to(pl|sh)|ts(70|m-|m3|m5)|tx-9|up(\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas-|your|zeto|zte-`, "i")
	tabletRegex        = jsregex.MustCompile(`tablet|ipad|xoom|sch-i800|kindle|silk|playbook`, "i")
	androidRegex       = jsregex.MustCompile(`android`, "i")
	mobileKeywordRegex = jsregex.MustCompile(`mobile`, "i")
)

// Phone models from the top brands; a match means a phone, not a tablet.
var knownPhonePatterns = []*jsregex.Regexp{
	jsregex.MustCompile(`redmi\s*(note|k|[0-9])`, "i"),
	jsregex.MustCompile(`poco\s*[a-z0-9]`, "i"),
	jsregex.MustCompile(`mi\s*([0-9]|note|mix|max)`, "i"),
	jsregex.MustCompile(`galaxy\s*(s|a|m|note)[0-9]`, "i"),
	jsregex.MustCompile(`galaxy\s*z\s*(fold|flip)`, "i"),
	jsregex.MustCompile(`huawei\s*(p|mate|nova|y)[0-9]`, "i"),
	jsregex.MustCompile(`honor\s*[0-9a-z]`, "i"),
	jsregex.MustCompile(`oppo\s*(a|f|find|reno|k)\s*[a-z0-9]`, "i"),
	jsregex.MustCompile(`vivo\s*(v|y|x|s|t|iqoo)[0-9]`, "i"),
	jsregex.MustCompile(`iqoo\s*[0-9a-z]`, "i"),
	jsregex.MustCompile(`oneplus\s*[0-9]`, "i"),
	jsregex.MustCompile(`one\s*plus\s*[0-9]`, "i"),
	jsregex.MustCompile(`pixel\s*[0-9]`, "i"),
	jsregex.MustCompile(`realme\s*[0-9a-z]`, "i"),
	jsregex.MustCompile(`moto\s*(g|e|x|z|edge|razr)`, "i"),
	jsregex.MustCompile(`motorola\s*(edge|razr)`, "i"),
	jsregex.MustCompile(`nokia\s*[0-9]`, "i"),
	jsregex.MustCompile(`xperia\s*[0-9a-z]`, "i"),
	jsregex.MustCompile(`nothing\s*phone`, "i"),
}

type brandPattern struct {
	pattern *jsregex.Regexp
	brand   string
}

// Order matters: more specific patterns come first.
var brandPatterns = []brandPattern{
	{jsregex.MustCompile(`xiaomi`, "i"), "Xiaomi"},
	{jsregex.MustCompile(`redmi`, "i"), "Xiaomi"},
	{jsregex.MustCompile(`poco`, "i"), "Xiaomi"},
	{jsregex.MustCompile(`samsung`, "i"), "Samsung"},
	{jsregex.MustCompile(`galaxy`, "i"), "Samsung"},
	{jsregex.MustCompile(`huawei`, "i"), "Huawei"},
	{jsregex.MustCompile(`honor`, "i"), "Honor"},
	{jsregex.MustCompile(`oppo`, "i"), "OPPO"},
	{jsregex.MustCompile(`vivo`, "i"), "Vivo"},
	{jsregex.MustCompile(`iqoo`, "i"), "Vivo"},
	{jsregex.MustCompile(`oneplus|one\s*plus`, "i"), "OnePlus"},
	{jsregex.MustCompile(`google`, "i"), "Google"},
	{jsregex.MustCompile(`pixel`, "i"), "Google"},
	{jsregex.MustCompile(`realme`, "i"), "Realme"},
	{jsregex.MustCompile(`motorola`, "i"), "Motorola"},
	{jsregex.MustCompile(`moto\s`, "i"), "Motorola"},
	{jsregex.MustCompile(`nokia`, "i"), "Nokia"},
	{jsregex.MustCompile(`sony`, "i"), "Sony"},
	{jsregex.MustCompile(`xperia`, "i"), "Sony"},
	{jsregex.MustCompile(`nothing`, "i"), "Nothing"},
	{jsregex.MustCompile(`\bapple\b(?!webkit)`, "i"), "Apple"},
	{jsregex.MustCompile(`\biphone\b`, "i"), "Apple"},
	{jsregex.MustCompile(`\bipad\b`, "i"), "Apple"},
	{jsregex.MustCompile(`lg[- ]`, "i"), "LG"},
	{jsregex.MustCompile(`zte`, "i"), "ZTE"},
	{jsregex.MustCompile(`lenovo`, "i"), "Lenovo"},
	{jsregex.MustCompile(`asus`, "i"), "ASUS"},
	{jsregex.MustCompile(`tcl`, "i"), "TCL"},
}

func detectBrand(ua string, model *string) *string {
	search := ua + " "
	if truthy(model) {
		search += *model
	}
	for _, candidate := range brandPatterns {
		if candidate.pattern.Test(search) {
			return str(candidate.brand)
		}
	}
	return nil
}

func isKnownPhoneModel(model string) bool {
	if model == "" {
		return false
	}
	for _, pattern := range knownPhonePatterns {
		if pattern.Test(model) {
			return true
		}
	}
	return false
}

// getDevice is the fallback device type when ua-parser-js reports none.
func getDevice(ua string, model *string) string {
	modelString := ""
	if model != nil {
		modelString = *model
	}
	if isKnownPhoneModel(modelString) || isKnownPhoneModel(ua) {
		return "mobile"
	}
	isSamsungMobile := samsungMobileRegex.Test(ua)
	if isSamsungMobile {
		return "mobile"
	}
	if samsungTabletRegex.Test(ua) {
		return "tablet"
	}
	isLGMobile := lgMobileRegex.Test(ua)
	if isLGMobile {
		return "mobile"
	}
	if mobileRegex1.Test(ua) || mobileRegex2.Test(jsregex.SliceUTF16(ua, 4)) {
		return "mobile"
	}
	isAndroid := androidRegex.Test(ua)
	hasMobileKeyword := mobileKeywordRegex.Test(ua)
	if tabletRegex.Test(ua) {
		return "tablet"
	}
	if isAndroid && !hasMobileKeyword {
		// Android without a mobile or tablet marker is far more often a phone.
		return "mobile"
	}
	return "desktop"
}
