<?php
// import_products.php
// CLI script to import products from a JSON file, validate, respect API rate limit (10 req/min),
// save valid products to DB, invalid ones to invalid_products with error details,
// show progress in console, and handle network failures with retries.

// CONFIGURATION
$dbDsn = 'mysql:host=127.0.0.1;dbname=your_db;charset=utf8mb4';
$dbUser = 'dbuser';
$dbPass = 'dbpass';

$inputFile = $argv[1] ?? 'products.json'; // JSON array of products
$maxApiRequestsPerMinute = 10;
$apiBaseUrl = 'https://external.example.com/enrich'; // örnek: ürün bilgilerini zenginleştirmek için kullanılabilir
$maxRetries = 3;

// ---- RateLimiter class (token bucket style) ----
class RateLimiter {
    private $capacity;
    private $tokens;
    private $refillIntervalSec;
    private $lastRefill;

    public function __construct(int $capacity, int $perSeconds) {
        $this->capacity = $capacity;
        $this->tokens = $capacity;
        $this->refillIntervalSec = $perSeconds / $capacity; // seconds per token
        $this->lastRefill = microtime(true);
    }

    private function refill() {
        $now = microtime(true);
        $elapsed = $now - $this->lastRefill;
        $tokensToAdd = floor($elapsed / $this->refillIntervalSec);
        if ($tokensToAdd > 0) {
            $this->tokens = min($this->capacity, $this->tokens + $tokensToAdd);
            $this->lastRefill += $tokensToAdd * $this->refillIntervalSec;
        }
    }

    // Blocks until a token is available
    public function waitForToken() {
        while (true) {
            $this->refill();
            if ($this->tokens >= 1) {
                $this->tokens -= 1;
                return;
            }
            // sleep a short time and loop
            usleep(250000); // 250ms
        }
    }
}

// ---- Utility: console progress ----
function printProgress($done, $total, $startTime) {
    $percent = $total > 0 ? round($done / $total * 100, 1) : 100;
    $elapsed = time() - $startTime;
    $perItem = $done > 0 ? $elapsed / $done : 0;
    $remaining = $perItem > 0 ? round(($total - $done) * $perItem) : 0;
    $barWidth = 40;
    $filled = (int)round($barWidth * $done / max(1, $total));
    $bar = str_repeat('=', $filled) . str_repeat(' ', $barWidth - $filled);
    echo sprintf("\r[%s] %d/%d (%s%%) Elapsed: %ds ETA: %ds", $bar, $done, $total, $percent, $elapsed, $remaining);
    if ($done === $total) echo PHP_EOL;
}

// ---- Product validation ----
function validateProduct(array $p): array {
    $errors = [];

    if (empty($p['sku'])) $errors[] = "sku boş olamaz";
    if (empty($p['name'])) $errors[] = "name boş olamaz";
    if (!isset($p['price'])) $errors[] = "price belirtilmeli";
    else if (!is_numeric($p['price']) || $p['price'] < 0) $errors[] = "price pozitif bir sayı olmalı";
    if (!isset($p['stock'])) $errors[] = "stock belirtilmeli";
    else if (!is_int($p['stock']) && !ctype_digit((string)$p['stock'])) $errors[] = "stock tam sayı olmalı";
    else if ((int)$p['stock'] < 0) $errors[] = "stock negatif olamaz";

    // ekstra: sku format kontrolü
    if (!empty($p['sku']) && !preg_match('/^[A-Za-z0-9\-_]+$/', $p['sku'])) {
        $errors[] = "sku sadece harf, rakam, '-' ve '_' içerebilir";
    }

    return $errors;
}

// ---- External API call with retries and rate limiting ----
function callExternalApi(array $product, PDO $pdo = null, RateLimiter $rateLimiter = null, $apiBaseUrl = '') {
    // Bu örnekte dış API ürünü "zenginleştirir" ve metadata döner.
    // Rate limiter varsa, kullan.
    if ($rateLimiter) $rateLimiter->waitForToken();

    $maxRetries = 3;
    $attempt = 0;
    $backoffBase = 1; // saniye

    while ($attempt < $maxRetries) {
        $attempt++;
        $ch = curl_init();
        $url = $apiBaseUrl . '?' . http_build_query(['sku' => $product['sku'] ?? '']);
        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => 10,
            CURLOPT_CONNECTTIMEOUT => 5,
            // eğer POST gerekiyorsa değiştirin
            // CURLOPT_POST => true,
            // CURLOPT_POSTFIELDS => json_encode($product),
            CURLOPT_HTTPHEADER => [
                'Accept: application/json',
                'User-Agent: ProductImporter/1.0'
            ],
        ]);

        $resp = curl_exec($ch);
        $curlErr = null;
        if ($resp === false) $curlErr = curl_error($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        if ($resp !== false && $httpCode >= 200 && $httpCode < 300) {
            $data = json_decode($resp, true);
            if (json_last_error() === JSON_ERROR_NONE) {
                return ['ok' => true, 'data' => $data];
            } else {
                // Bozuk JSON -> treat as error and maybe retry
                $err = "Invalid JSON from API";
            }
        } else {
            $err = $curlErr ?? "HTTP $httpCode";
        }

        // log attempt (optionally to DB/log file)
        // sleep exponential backoff before retry
        $sleep = $backoffBase * (2 ** ($attempt - 1));
        sleep($sleep);
    }

    return ['ok' => false, 'error' => $err ?? 'Unknown error after retries'];
}

// ---- MAIN ----
$start = time();

if (!file_exists($inputFile)) {
    fwrite(STDERR, "Hata: Dosya bulunamadı: $inputFile\n");
    exit(1);
}

$json = file_get_contents($inputFile);
$items = json_decode($json, true);
if (!is_array($items)) {
    fwrite(STDERR, "Hata: Girdi JSON'u dizi içermiyor veya geçersiz JSON.\n");
    exit(1);
}

try {
    $pdo = new PDO($dbDsn, $dbUser, $dbPass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_EMULATE_PREPARES => false,
    ]);
} catch (PDOException $e) {
    fwrite(STDERR, "DB bağlantı hatası: " . $e->getMessage() . PHP_EOL);
    exit(1);
}

// prepare statements
$insertProductStmt = $pdo->prepare("
    INSERT INTO products (sku, name, description, price, stock, metadata)
    VALUES (:sku, :name, :description, :price, :stock, :metadata)
    ON DUPLICATE KEY UPDATE name=VALUES(name), description=VALUES(description), price=VALUES(price), stock=VALUES(stock), metadata=VALUES(metadata)
");

$insertInvalidStmt = $pdo->prepare("
    INSERT INTO invalid_products (raw_data, errors)
    VALUES (:raw_data, :errors)
");

// rate limiter: capacity 10 tokens per 60 seconds
$rateLimiter = new RateLimiter($maxApiRequestsPerMinute, 60);

$total = count($items);
$done = 0;
$success = 0;
$invalidCount = 0;
$failedApi = 0;

foreach ($items as $i => $item) {
    $done++;
    // normalize numeric types
    if (isset($item['stock']) && !is_int($item['stock'])) $item['stock'] = (int)$item['stock'];
    if (isset($item['price']) && !is_float($item['price'])) $item['price'] = (float)$item['price'];

    // 1) validate
    $errors = validateProduct($item);
    if (!empty($errors)) {
        $invalidCount++;
        $insertInvalidStmt->execute([
            ':raw_data' => json_encode($item, JSON_UNESCAPED_UNICODE),
            ':errors' => json_encode($errors, JSON_UNESCAPED_UNICODE),
        ]);
        printProgress($done, $total, $start);
        continue;
    }

    // 2) optional external API enrich (respect rate limit)
    $enriched = null;
    if (!empty($apiBaseUrl)) {
        $apiResult = callExternalApi($item, $pdo, $rateLimiter, $apiBaseUrl);
        if ($apiResult['ok']) {
            $enriched = $apiResult['data'];
        } else {
            // API başarısız olursa: retry yapılmış ama başarısızsa -> kaydet ve devam et
            $failedApi++;
            $insertInvalidStmt->execute([
                ':raw_data' => json_encode($item, JSON_UNESCAPED_UNICODE),
                ':errors' => json_encode(['api_error' => $apiResult['error']], JSON_UNESCAPED_UNICODE),
            ]);
            printProgress($done, $total, $start);
            continue;
        }
    }

    // 3) insert into DB (transactional per-row to avoid partial insertion)
    try {
        $pdo->beginTransaction();
        $insertProductStmt->execute([
            ':sku' => $item['sku'],
            ':name' => $item['name'],
            ':description' => $item['description'] ?? null,
            ':price' => $item['price'],
            ':stock' => $item['stock'],
            ':metadata' => json_encode($enriched ?? new stdClass(), JSON_UNESCAPED_UNICODE),
        ]);
        $pdo->commit();
        $success++;
    } catch (Exception $e) {
        $pdo->rollBack();
        $invalidCount++;
        $insertInvalidStmt->execute([
            ':raw_data' => json_encode($item, JSON_UNESCAPED_UNICODE),
            ':errors' => json_encode(['db_error' => $e->getMessage()], JSON_UNESCAPED_UNICODE),
        ]);
    }

    // print progress
    printProgress($done, $total, $start);
}

$elapsed = time() - $start;
echo PHP_EOL . "İçe aktarma tamamlandı. Toplam: $total, Başarılı: $success, Geçersiz: $invalidCount, API hatasıyla kaydedilen: $failedApi, Süre: {$elapsed}s\n";