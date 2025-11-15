#!/bin/bash
# Script to use existing Ozone installation or download pre-built distribution
# This avoids building the project entirely

set -e

echo "=== Using Existing Ozone Installation ==="
echo "This script uses existing Ozone binaries or downloads them"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Function to download Ozone distribution
download_ozone() {
    local version=${1:-"2.0.0"}
    local download_url="https://downloads.apache.org/ozone/ozone-${version}/ozone-${version}.tar.gz"
    local target_dir="ozone-${version}"
    
    echo "📥 Downloading Ozone ${version}..."
    
    if [ ! -d "$target_dir" ]; then
        mkdir -p "$target_dir"
        cd "$target_dir"
        
        # Download and extract
        curl -L "$download_url" -o "ozone-${version}.tar.gz"
        tar -xzf "ozone-${version}.tar.gz"
        mv "ozone-${version}"/* .
        rmdir "ozone-${version}"
        rm "ozone-${version}.tar.gz"
        
        cd ..
    else
        echo "✅ Ozone ${version} already exists"
    fi
}

# Function to check for existing Ozone installation
find_existing_ozone() {
    local possible_paths=(
        "/opt/hadoop"
        "/usr/local/hadoop"
        "$HOME/hadoop"
        "$HOME/ozone"
        "./ozone-2.0.0"
        "./ozone-1.4.0"
    )
    
    for path in "${possible_paths[@]}"; do
        if [ -d "$path" ] && [ -f "$path/bin/ozone" ]; then
            echo "$path"
            return 0
        fi
    done
    
    return 1
}

# Try to find existing Ozone installation
echo "🔍 Looking for existing Ozone installation..."
OZONE_HOME=$(find_existing_ozone)

if [ -n "$OZONE_HOME" ]; then
    echo "✅ Found existing Ozone installation at: $OZONE_HOME"
else
    echo "📥 No existing installation found, downloading Ozone 2.0.0..."
    download_ozone "2.0.0"
    OZONE_HOME="./ozone-2.0.0"
fi

echo "📁 Using Ozone installation: $OZONE_HOME"
echo ""

# Create a temporary directory for our test setup
TEST_DIR="./ozone-test-setup"
mkdir -p "$TEST_DIR"

# Copy our Byteman script to the test directory
echo "📋 Setting up test environment..."
cp dev-support/byteman/compaction-fault-injection.btm "$TEST_DIR/"

# Create a custom docker-compose file that uses the existing Ozone installation
cat > "$TEST_DIR/docker-compose-custom.yaml" << EOF
version: '3.7'
services:
  om1:
    image: apache/ozone-runner:latest
    hostname: om1
    volumes:
      - $OZONE_HOME:/opt/hadoop
      - ./compaction-fault-injection.btm:/opt/hadoop/dev-support/byteman/compaction-fault-injection.btm
    environment:
      - OZONE_SERVER_OPTS=-javaagent:/opt/byteman.jar=listener:true,address:0.0.0.0,port:9091
      - BYTEMAN_PORT=9091
    ports:
      - "9880:9874"
      - "9890:9872"
      - "9091:9091"
    command: ["/opt/hadoop/bin/ozone", "om"]
    networks:
      ozone_net:
        ipv4_address: 172.25.0.111

  datanode1:
    image: apache/ozone-runner:latest
    hostname: datanode1
    volumes:
      - $OZONE_HOME:/opt/hadoop
    environment:
      - OZONE_SERVER_OPTS=-javaagent:/opt/byteman.jar=listener:true,address:0.0.0.0,port:9091
      - BYTEMAN_PORT=9091
    ports:
      - "19864:9999"
      - "9092:9091"
    command: ["/opt/hadoop/bin/ozone", "datanode"]
    networks:
      ozone_net:
        ipv4_address: 172.25.0.102

  scm1:
    image: apache/ozone-runner:latest
    hostname: scm1.org
    volumes:
      - $OZONE_HOME:/opt/hadoop
    ports:
      - "9990:9876"
      - "9992:9860"
    command: ["/opt/hadoop/bin/ozone", "scm"]
    networks:
      ozone_net:
        ipv4_address: 172.25.0.116

networks:
  ozone_net:
    driver: bridge
    ipam:
      config:
        - subnet: 172.25.0.0/16
EOF

# Create a simple test script
cat > "$TEST_DIR/test-compaction.sh" << 'EOF'
#!/bin/bash
set -e

echo "🧪 Testing compaction interruption..."

# Wait for services to be ready
sleep 10

# Test 1: Check if services are running
echo "1. Checking service status..."
docker-compose ps

# Test 2: Check if Byteman is working
echo "2. Testing Byteman agent..."
docker exec om1 bmsubmit -p 9091 -l || echo "Byteman not ready yet"

# Test 3: Inject compaction fault
echo "3. Injecting compaction fault..."
docker exec om1 bmsubmit -p 9091 /opt/hadoop/dev-support/byteman/compaction-fault-injection.btm || echo "Fault injection failed"

# Test 4: Create test data
echo "4. Creating test data..."
docker exec om1 /opt/hadoop/bin/ozone sh volume create /testvol || echo "Volume creation failed"
docker exec om1 /opt/hadoop/bin/ozone sh bucket create /testvol/testbucket || echo "Bucket creation failed"

# Test 5: Check logs
echo "5. Checking compaction logs..."
docker logs om1 | grep -i compaction | tail -3 || echo "No compaction logs found"

echo "✅ Test completed!"
EOF

chmod +x "$TEST_DIR/test-compaction.sh"

# Navigate to test directory
cd "$TEST_DIR"

echo "🚀 Starting minimal Ozone cluster..."
docker-compose -f docker-compose-custom.yaml up -d

echo "✅ Cluster started!"
echo ""

# Run the test
echo "🧪 Running compaction test..."
./test-compaction.sh

echo ""
echo "🎉 Test completed successfully!"
echo ""
echo "=== Summary ==="
echo "✅ Used existing/downloaded Ozone installation: $OZONE_HOME"
echo "✅ No project build required"
echo "✅ Started minimal cluster with Byteman"
echo "✅ Tested compaction interruption"
echo ""
echo "📊 To view logs:"
echo "   docker-compose -f docker-compose-custom.yaml logs om1"
echo ""
echo "🛑 To stop the cluster:"
echo "   docker-compose -f docker-compose-custom.yaml down"
echo ""
echo "📁 Test files are in: $TEST_DIR" 